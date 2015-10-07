package com.hazelcast.query.extractor;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.Extractors;
import com.hazelcast.query.impl.PortableExtractor;
import com.hazelcast.query.impl.getters.ReflectionHelper;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

public class ExtractionEngine {

    public static Object extractAttribute(Extractors extractors, String attributeName, Object key, Object value, SerializationService ss) {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return ss.toObject(key);
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return ss.toObject(value);
        }

        boolean isKey = isKey(attributeName);
        attributeName = getAttributeName(isKey, attributeName);
        Object target = isKey ? key : value;

        if (target instanceof Portable || target instanceof Data) {
            Data targetData = ss.toData(target);
            if (targetData.isPortable()) {
                return extractViaPortable(attributeName, targetData, ss);
            }
        }

        Object targetObject = ss.toObject(target);

        ValueExtractor extractor = extractors.getExtractor(attributeName);
        if (extractor != null) {
            return extractor.extract(targetObject);
        } else {
            return extractViaReflection(attributeName, targetObject);
        }
    }

    private static boolean isKey(String attributeName) {
        return attributeName.startsWith(KEY_ATTRIBUTE_NAME);
    }

    private static String getAttributeName(boolean isKey, String attributeName) {
        if (isKey) {
            return attributeName.substring(KEY_ATTRIBUTE_NAME.length() + 1);
        } else {
            return attributeName;
        }
    }

    private static Comparable extractViaPortable(String attributeName, Data data, SerializationService ss) {
        try {
            return PortableExtractor.extractValue(ss, data, attributeName);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    // This method is very inefficient because:
    // lot of time is spend on retrieving field/method and it isn't cached
    // the actual invocation on the Field, Method is also is quite expensive.
    private static Object extractViaReflection(String attributeName, Object obj) {
        try {
            return ReflectionHelper.extractValue(obj, attributeName);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    public static AttributeType extractAttributeType(Extractors extractors, String attributeName, Object key, Object value, SerializationService ss) {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return ReflectionHelper.getAttributeType(key.getClass());
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return ReflectionHelper.getAttributeType(value.getClass());
        }

//        boolean isKey = ExtractionEngine.isKey(attributeName);
//        attributeName = getAttributeName(isKey, attributeName);
//
//        Object target = isKey ? key : value;
//
//        if (target instanceof Portable || target instanceof Data) {
//            Data data = ss.toData(target);
//            if (data.isPortable()) {
//                PortableContext portableContext = ss.getPortableContext();
//                return PortableExtractor.getAttributeType(portableContext, data, attributeName);
//            }
//        }
//
//        ValueExtractor extractor = extractors.getExtractor(attributeName);
//        if (extractor != null) {
//            Object targetObject = ss.toObject(target);
//            Object extractedObject = extractor.extract(targetObject);
//            return getExtractedAttributeType(extractedObject);
//        } else {
//            return ReflectionHelper.getAttributeType(isKey ? key : value, attributeName);
//        }

        // TODO Can we do it like this for single-value attributes?
        // ReflectionHelper can return a MultiResult now, right?
        // If so we need to do the extraction to check the type of the objects from MultiResult.
        Object extractedObject = extractAttribute(extractors, attributeName, key, value, ss);
        return getExtractedAttributeType(extractedObject);
    }

    private static AttributeType getExtractedAttributeType(Object extractedObject) {
        if (extractedObject instanceof MultiResult) {
            return getExtractedAttributeTypeFromMultiResult((MultiResult) extractedObject);
        } else {
            return getExtractedAttributeTypeFromSingleResult(extractedObject);
        }
    }

    private static AttributeType getExtractedAttributeTypeFromSingleResult(Object extractedObject) {
        if (extractedObject == null) {
            return null;
        } else {
            return ReflectionHelper.getAttributeType(extractedObject.getClass());
        }
    }

    private static AttributeType getExtractedAttributeTypeFromMultiResult(MultiResult extractedMultiResult) {
        if (extractedMultiResult.isEmpty()) {
            return null;
        } else {
            return ReflectionHelper.getAttributeType(extractedMultiResult.getResults().get(0).getClass());
        }
    }

}
