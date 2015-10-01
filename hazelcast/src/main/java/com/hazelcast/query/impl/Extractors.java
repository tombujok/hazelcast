package com.hazelcast.query.impl;

import com.hazelcast.config.MapExtractorConfig;
import com.hazelcast.query.extractor.ValueExtractor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Extractors {

    private static final Extractors EMPTY = new Extractors(Collections.<MapExtractorConfig>emptyList());

    private final Map<String, ValueExtractor> extractors;

    public Extractors(List<MapExtractorConfig> mapExtractorConfigs) {
        extractors = new HashMap<String, ValueExtractor>();
        for (MapExtractorConfig config : mapExtractorConfigs) {
            if (extractors.containsKey(config.getAttribute())) {
                throw new IllegalArgumentException("Could not add " + config + ". Extractor for this attribute already added.");
            }
            extractors.put(config.getAttribute(), instantiateExtractor(config));
        }
    }

    private ValueExtractor instantiateExtractor(MapExtractorConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getType());
            Object extractor = clazz.newInstance();
            if (extractor instanceof ValueExtractor) {
                return (ValueExtractor) extractor;
            } else {
                throw new IllegalArgumentException("Extractor does not extend ValueExtractor class " + config);
            }
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        }
    }

    public ValueExtractor getExtractor(String attribute) {
        return extractors.get(attribute);
    }

    public static Extractors empty() {
        return EMPTY;
    }

}
