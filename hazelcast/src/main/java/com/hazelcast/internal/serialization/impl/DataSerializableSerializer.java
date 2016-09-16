/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl;


import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.Versioned;
import com.hazelcast.nio.serialization.VersionedDataSerializableFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;
import com.hazelcast.util.collection.Int2ObjectHashMap;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;

/**
 * The {@link StreamSerializer} that handles:
 * <ol>
 * <li>{@link DataSerializable}</li>
 * <li>{@link IdentifiedDataSerializable}</li>
 * </ol>
 * <p/>
 * Due to the operation responding on deserialization errors this class
 * has a dependency to {@link com.hazelcast.nio.IOUtil#extractOperationCallId(com.hazelcast.nio.serialization.Data,
 * com.hazelcast.internal.serialization.InternalSerializationService)}.
 * If the way the DataSerializer serializes values is changed the extract method needs to be changed too!
 */
final class DataSerializableSerializer implements StreamSerializer<DataSerializable> {

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    private final Int2ObjectHashMap<DataSerializableFactory> factories = new Int2ObjectHashMap<DataSerializableFactory>();
//    private final Int2ObjectHashMap<VersionedDataSerializableFactory> versionedFactories = new Int2ObjectHashMap<VersionedDataSerializableFactory>();

    private final Node node;

    DataSerializableSerializer(Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                               ClassLoader classLoader, Node node) {
        this.node = node;
        try {
            final Iterator<DataSerializerHook> hooks = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID, classLoader);
            while (hooks.hasNext()) {
                DataSerializerHook hook = hooks.next();
                final DataSerializableFactory factory = hook.createFactory();
                if (factory != null) {
                    register(hook.getFactoryId(), factory);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        if (dataSerializableFactories != null) {
            for (Map.Entry<Integer, ? extends DataSerializableFactory> entry : dataSerializableFactories.entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        }
    }

    private void register(int factoryId, DataSerializableFactory factory) {
        final DataSerializableFactory current = factories.get(factoryId);
        if (current != null) {
            if (current.equals(factory)) {
                Logger.getLogger(getClass()).warning("DataSerializableFactory[" + factoryId + "] is already registered! Skipping "
                        + factory);
            } else {
                throw new IllegalArgumentException("DataSerializableFactory[" + factoryId + "] is already registered! "
                        + current + " -> " + factory);
            }
        } else {
//            if (factory instanceof VersionedDataSerializableFactory) {
//                versionedFactories.put(factoryId, (VersionedDataSerializableFactory) factory);
//            } else {
            factories.put(factoryId, (DataSerializableFactory) factory);
//            }
        }
    }

    @Override
    public int getTypeId() {
        return CONSTANT_TYPE_DATA_SERIALIZABLE;
    }

    @Override
    public DataSerializable read(ObjectDataInput in) throws IOException {
        final DataSerializable ds;
        final byte identified = in.readByte();
        int id = 0;
        int factoryId = 0;
        String className = null;
        try {
            if (identified == 0 || identified == 3) {
                className = in.readUTF();
                ds = ClassLoaderUtil.newInstance(in.getClassLoader(), className);

                if (identified == 3) {
                    byte version = in.readByte();
                    Version v = node.getClusterService().getClusterVersion();
                    Version objectVersion = Version.of(v.getMajor(), version, v.getPatch());
                    setVersion(in, objectVersion);
                }

                ds.readData(in);
            } else {
                // If you ever change the way this is serialized think about to change
                // BasicOperationService::extractOperationCallId
                factoryId = in.readInt();
                final DataSerializableFactory dsf = factories.get(factoryId);
                if (dsf == null) {
                    throw new HazelcastSerializationException("No DataSerializerFactory registered for namespace: " + factoryId);
                }
                id = in.readInt();

                // TODO: @mm - we can check if DS class is final.

                if (identified == 2) {
                    // My brilliance amuses even myself :) (just kidding)
                    byte version = in.readByte();
                    Version v = node.getClusterService().getClusterVersion();
                    Version objectVersion = Version.of(v.getMajor(), version, v.getPatch());

                    if (dsf instanceof VersionedDataSerializableFactory) {
                        ds = ((VersionedDataSerializableFactory) dsf).create(id, objectVersion);
                    } else {
                        ds = dsf.create(id);
                    }

                    setVersion(in, objectVersion);
                } else {
                    // unversioned but identified
                    ds = dsf.create(id);
                    setVersion(in, Version.UNKNOWN);
                }

                if (ds == null) {
                    throw new HazelcastSerializationException(dsf
                            + " is not be able to create an instance for id: " + id + " on factoryId: " + factoryId);
                }

                ds.readData(in);
            }
//            else {
//                // If you ever change the way this is serialized think about to change
//                // BasicOperationService::extractOperationCallId
//                factoryId = in.readInt(); // can be compressed to short
//                final VersionedDataSerializableFactory dsf = versionedFactories.get(factoryId);
//                if (dsf == null) {
//                    throw new HazelcastSerializationException("No DataSerializerFactory registered for namespace: " + factoryId);
//                }
//                id = in.readInt();
//                byte version = in.readByte(); // can be compressed to short
//                Versioned vds = dsf.create(id, version);
//                if (vds == null) {
//                    throw new HazelcastSerializationException(dsf
//                            + " is not be able to create an instance for id: " + id + " on factoryId: " + factoryId);
//                }
//
//                in.setVersion(version);
//
//                // TODO: @mm - we can check if DS class is final.
//                vds.readData(in);
//                return vds;
//            }

            return ds;
        } catch (Exception e) {
            throw rethrowReadException(id, factoryId, className, e);
        }
    }

    private IOException rethrowReadException(int id, int factoryId, String className, Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException("Problem while reading DataSerializable, namespace: "
                + factoryId
                + ", id: " + id
                + ", class: '" + className + "'"
                + ", exception: " + e.getMessage(), e);
    }

    @Override
    public void write(ObjectDataOutput out, DataSerializable obj) throws IOException {
        Version version = node.getClusterService().getClusterVersion();
        // If you ever change the way this is serialized think about to change
        // BasicOperationService::extractOperationCallId
        byte identified = 0;
        if (obj instanceof IdentifiedDataSerializable) {
            if (obj instanceof Versioned) {
                identified = 2;
            } else {
                identified = 1;
            }
        } else if (obj instanceof Versioned) {
            identified = 3;
        }


        out.writeByte(identified);
        if (identified == 0 || identified == 3) {
            out.writeUTF(obj.getClass().getName());
            if (identified == 3) {
                out.writeByte(version.getMinor());
            }
        } else if (identified == 1 || identified == 2) {
            final IdentifiedDataSerializable ds = (IdentifiedDataSerializable) obj;
            out.writeInt(ds.getFactoryId());
            out.writeInt(ds.getId());
            if (identified == 2) {
                out.writeByte(version.getMinor());
            }
        }
//        else {
//            final Versioned ds = (Versioned) obj;
//            out.writeShort(ds.getFactoryId());
//            out.writeShort(ds.getId());
//            out.writeByte(ds.getVersion());
//        }

        setVersion(out, version);
        obj.writeData(out);
    }

    // UGLY!!! I know but it's just a PoC
    private void setVersion(ObjectDataOutput out, Version version) {
        if (out instanceof UnsafeObjectDataOutput) {
            ((UnsafeObjectDataOutput) out).setVersion(version);
        } else if (out instanceof ByteArrayObjectDataOutput) {
            ((ByteArrayObjectDataOutput) out).setVersion(version);
        } else if (out instanceof ObjectDataOutputStream) {
            ((ObjectDataOutputStream) out).setVersion(version);

        } else {
            throw new IllegalArgumentException("Illegal output stream implementation " + out.getClass());
        }
    }

    // UGLY!!! I know but it's just a PoC
    private void setVersion(ObjectDataInput in, Version version) {
        if (in instanceof UnsafeObjectDataInput) {
            ((UnsafeObjectDataInput) in).setVersion(version);
        } else if (in instanceof ObjectDataInputStream) {
            ((ObjectDataInputStream) in).setVersion(version);
        } else if (in instanceof ByteArrayObjectDataInput) {
            ((ByteArrayObjectDataInput) in).setVersion(version);
        } else {
            throw new IllegalArgumentException("Illegal input stream implementation " + in.getClass());
        }
    }

    @Override
    public void destroy() {
        factories.clear();
    }
}
