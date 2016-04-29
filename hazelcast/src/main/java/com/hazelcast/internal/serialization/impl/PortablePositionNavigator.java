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

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.internal.serialization.impl.PortableHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.internal.serialization.impl.PortableHelper.extractAttributeNameNameWithoutArguments;
import static com.hazelcast.internal.serialization.impl.PortablePositionFactory.checkFactoryAndClass;

final class PortablePositionNavigator {

    private static final Pattern NESTED_PATH_SPLITTER = Pattern.compile("\\.");

    // cache of commonly returned values to avoid extra allocations
    private static final PortableSinglePosition NIL_NOT_LAST = PortableSinglePosition.nil(false);
    private static final PortableSinglePosition NIL_LAST_ANY = PortableSinglePosition.nil(true, true);
    private static final PortableSinglePosition NIL_NOT_LAST_ANY = PortableSinglePosition.nil(false, true);
    private static final PortableSinglePosition EMPTY_LAST_ANY = PortableSinglePosition.empty(true, true);
    private static final PortableSinglePosition EMPTY_NOT_LAST_ANY = PortableSinglePosition.empty(false, true);

    // invariants
    private BufferObjectDataInput in;
    private int offset;
    private int finalPosition;
    private ClassDefinition cd;
    private PortableSerializer serializer;

    // holds the initial state for reset
    private final int initPosition;
    private final ClassDefinition initCd;
    private final int initOffset;
    private final int initFinalPosition;
    private PortableSerializer initSerializer;

    private Deque<NavigationFrame> multiPositions;

    PortablePositionNavigator(BufferObjectDataInput in, ClassDefinition cd, PortableSerializer serializer) {
        this.in = in;
        this.cd = cd;
        this.serializer = serializer;

        initFieldCountAndOffset(in, cd);

        this.initCd = cd;
        this.initSerializer = serializer;

        this.initPosition = in.position();
        this.initFinalPosition = finalPosition;
        this.initOffset = offset;
    }

    private void initFieldCountAndOffset(BufferObjectDataInput in, ClassDefinition cd) {
        int fieldCount;
        try {
            // final position after portable is read
            finalPosition = in.readInt();
            fieldCount = in.readInt();
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        }
        if (fieldCount != cd.getFieldCount()) {
            throw new IllegalStateException("Field count[" + fieldCount + "] in stream does not match " + cd);
        }
        offset = in.position();
    }

    int getFinalPosition() {
        return finalPosition;
    }

    int getOffset() {
        return offset;
    }

    PortablePosition findPositionOf(String path) throws IOException {
        try {
            return findFieldPosition(path);
        } finally {
            reset();
        }
    }

    private void reset() {
        cd = initCd;
        serializer = initSerializer;
        in.position(initPosition);
        finalPosition = initFinalPosition;
        offset = initOffset;
        if (multiPositions != null) {
            multiPositions.clear();
        }
    }

    private PortablePosition findFieldPosition(String nestedPath) throws IOException {
        String[] pathTokens = NESTED_PATH_SPLITTER.split(nestedPath);

        PortablePosition result = null;
        for (int i = 0; i < pathTokens.length; i++) {
            result = processPath(pathTokens, i, nestedPath, null);
            if (result != null && result.isNullOrEmpty()) {
                break;
            }
        }

        if (result == null) {
            throw unknownFieldException(nestedPath);
        }

        // we didn't touch any [any] quantifier
        if (multiPositions == null || multiPositions.isEmpty()) {
            return processFieldPositionNoMultiPositions(nestedPath, result);

        } else {
            return processFieldPositionWithMultiPositions(nestedPath, pathTokens, result);

        }
    }

    private PortablePosition processFieldPositionNoMultiPositions(String nestedPath, PortablePosition result) {
        // for consistency: [any] queries always return a multi-result, even if there's a single result only.
        // The only case where it returns a PortableSingleResult is when the position is a a single null result.
        if (!result.isNullOrEmpty()) {
            if (isAnyPath(nestedPath)) {
                List<PortablePosition> positions = new LinkedList<PortablePosition>();
                positions.add(result);
                return new PortableMultiPosition(positions);
            }
        }
        return result;
    }

    private PortablePosition processFieldPositionWithMultiPositions(String nestedPath, String[] pathTokens,
                                                                    PortablePosition result) throws IOException {
        // we process the all the paths gathered due to [any] quantifiers
        List<PortablePosition> positions = new LinkedList<PortablePosition>();
        positions.add(result);
        while (!multiPositions.isEmpty()) {
            NavigationFrame frame = multiPositions.pollFirst();
            setupNavigatorForFrame(frame);
            for (int i = frame.pathTokenIndex; i < pathTokens.length; i++) {
                result = processPath(pathTokens, i, nestedPath, frame);
                if (result != null && result.isNullOrEmpty()) {
                    break;
                }
                frame = null;
            }
            if (result == null) {
                throw unknownFieldException(nestedPath);
            }
            positions.add(result);
        }
        return new PortableMultiPosition(positions);
    }

    private PortablePosition processPath(String[] pathTokens, int pathTokenIndex, String nestedPath,
                                         NavigationFrame frame) throws IOException {
        String token = pathTokens[pathTokenIndex];
        String field = extractAttributeNameNameWithoutArguments(token);
        FieldDefinition fd = cd.getField(field);
        boolean last = (pathTokenIndex == pathTokens.length - 1);

        if (fd == null || token == null) {
            throw unknownFieldException(field + " in " + nestedPath);
        }

        if (isPathTokenWithoutQuantifier(token)) {
            PortablePosition position = processPathTokenWithoutQuantifier(fd, last, nestedPath);
            if (position != null) {
                return position;
            }
        } else if (isPathTokenWithAnyQuantifier(token)) {
            PortablePosition position = processPathTokenWithAnyQuantifier(pathTokenIndex, nestedPath, frame, fd, last);
            if (position != null) {
                return position;
            }
        } else {
            PortablePosition position = processPathTokenWithNumberQuantifier(nestedPath, token, fd, last);
            if (position != null) {
                return position;
            }

        }

        return null;
    }

    private PortablePosition processPathTokenWithoutQuantifier(FieldDefinition fd, boolean last, String nestedPath)
            throws IOException {
        //
        // TOKEN without quantifier
        //
        if (last) {
            return readPositionOfCurrentElement(fd, nestedPath);
        } else {
            if (!advanceNavigatorToNextPortableTokenFromNonArrayElement(fd)) {
                return NIL_NOT_LAST;
            }
        }
        return null;
    }

    private PortablePosition processPathTokenWithAnyQuantifier(int pathTokenIndex, String nestedPath,
                                                               NavigationFrame frame, FieldDefinition fd,
                                                               boolean last) throws IOException {
        //
        // TOKEN with [any] quantifier
        //
        validateArrayType(fd, nestedPath);
        if (fd.getType() == FieldType.PORTABLE_ARRAY) {
            // PORTABLE array
            if (frame == null) {
                int len = getCurrentArrayLength(fd);
                if (len == 0) {
                    return emptyPosition(last);
                } else if (len == Bits.NULL_ARRAY_LENGTH) {
                    return nilPosition(last);
                }

                populateAnyNavigationFrames(pathTokenIndex, len);
                if (last) {
                    return readPositionOfCurrentElement(fd, 0, nestedPath);
                }
                advanceNavigatorToNextPortableTokenFromPortableArrayCell(fd, 0);

            } else {
                if (last) {
                    return readPositionOfCurrentElement(fd, frame.arrayIndex, nestedPath);
                }

                // check len
                advanceNavigatorToNextPortableTokenFromPortableArrayCell(fd, frame.arrayIndex);
            }
        } else {
            // PRIMITIVE array
            if (frame == null) {
                if (last) {
                    int len = getCurrentArrayLength(fd);
                    if (len == 0) {
                        return emptyPosition(last);
                    } else if (len == Bits.NULL_ARRAY_LENGTH) {
                        return nilPosition(last);
                    }
                    populateAnyNavigationFrames(pathTokenIndex, len);
                    return readPositionOfCurrentElement(fd, 0, nestedPath);
                }
                throw wrongUseOfAnyOperationException(nestedPath);
            } else {
                if (last) {
                    return readPositionOfCurrentElement(fd, frame.arrayIndex, nestedPath);
                }
                throw wrongUseOfAnyOperationException(nestedPath);
            }
        }
        return null;
    }

    private PortablePosition processPathTokenWithNumberQuantifier(String nestedPath, String token, FieldDefinition fd,
                                                                  boolean last) throws IOException {
        //
        // TOKEN with [number] quantifier
        //
        validateArrayType(fd, nestedPath);
        String quantifier = extractArgumentsFromAttributeName(token);
        if (quantifier == null) {
            throw new IllegalArgumentException("Malformed quantifier " + quantifier + " in " + nestedPath);
        }
        int index = Integer.valueOf(quantifier);
        if (index < 0) {
            throw new IllegalArgumentException("Array index " + index + " cannot be negative in " + nestedPath);
        }

        int len = getCurrentArrayLength(fd);
        if (len == 0) {
            return emptyPosition(last);
        } else if (len == Bits.NULL_ARRAY_LENGTH) {
            return nilPosition(last);
        } else if (index >= len) {
            return nilPosition(last);
        } else {
            if (last) {
                return readPositionOfCurrentElement(fd, index, nestedPath);
            } else if (fd.getType() == FieldType.PORTABLE_ARRAY) {
                advanceNavigatorToNextPortableTokenFromPortableArrayCell(fd, index);
            }
        }
        return null;
    }

    private void populateAnyNavigationFrames(int pathTokenIndex, int len) {
        // populate "recursive" multi-positions
        if (multiPositions == null) {
            multiPositions = new ArrayDeque<NavigationFrame>();
        }
        for (int k = len - 1; k > 0; k--) {
            multiPositions.addFirst(new NavigationFrame(cd, pathTokenIndex, k, in.position(), this.offset));
        }
    }

    private void setupNavigatorForFrame(NavigationFrame frame) {
        in.position(frame.streamPosition);
        offset = frame.streamOffset;
        cd = frame.cd;
    }

    private void advanceNavigatorToNextPortableTokenFromPortableArrayCell(FieldDefinition fd, int index)
            throws IOException {
        int pos = calculateStreamPositionFromMetadata(fd);
        in.position(pos);

        // read array length and ignore
        in.readInt();
        int factoryId = in.readInt();
        int classId = in.readInt();

        checkFactoryAndClass(fd, factoryId, classId);

        final int coffset = in.position() + index * Bits.INT_SIZE_IN_BYTES;
        in.position(coffset);
        int portablePosition = in.readInt();
        in.position(portablePosition);
        int versionId = in.readInt();

        doAdvanceNavigatorToNextPortableToken(factoryId, classId, versionId);
    }

    // returns true if managed to advance, false if advance failed due to null field
    private boolean advanceNavigatorToNextPortableTokenFromNonArrayElement(FieldDefinition fd) throws IOException {
        int pos = calculateStreamPositionFromMetadata(fd);
        in.position(pos);
        boolean isNull = in.readBoolean();
        if (isNull) {
            return false;
        }

        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();
        doAdvanceNavigatorToNextPortableToken(factoryId, classId, version);
        return true;
    }

    private void doAdvanceNavigatorToNextPortableToken(int factoryId, int classId, int version) throws IOException {
        cd = serializer.setupPositionAndDefinition(in, factoryId, classId, version);
        initFieldCountAndOffset(in, cd);
    }

    private PortablePosition readPositionOfCurrentElement(FieldDefinition fd, String path)
            throws IOException {
        return PortablePositionFactory.createSinglePositionForReadAccess(fd, calculateStreamPositionFromMetadata(fd),
                true, in, path);
    }

    private PortablePosition readPositionOfCurrentElement(FieldDefinition fd, int index, String path)
            throws IOException {
        return PortablePositionFactory.createSinglePositionForReadAccess(fd, calculateStreamPositionFromMetadata(fd),
                true, index, in, path);
    }


    private int calculateStreamPositionFromMetadata(FieldDefinition fd) throws IOException {
        int pos = in.readInt(offset + fd.getIndex() * Bits.INT_SIZE_IN_BYTES);
        short len = in.readShort(pos);
        // name + len + type
        return pos + Bits.SHORT_SIZE_IN_BYTES + len + 1;
    }

    private int getCurrentArrayLength(FieldDefinition fd) throws IOException {
        int originalPos = in.position();
        try {
            int pos = calculateStreamPositionFromMetadata(fd);
            in.position(pos);
            return in.readInt();
        } finally {
            in.position(originalPos);
        }
    }

    private PortablePosition nilPosition(boolean last) {
        return last ? NIL_LAST_ANY : NIL_NOT_LAST_ANY;
    }

    private PortablePosition emptyPosition(boolean last) {
        return last ? EMPTY_LAST_ANY : EMPTY_NOT_LAST_ANY;
    }

    private boolean isPathTokenWithoutQuantifier(String pathToken) {
        return !pathToken.endsWith("]");
    }

    private boolean isAnyPath(String path) {
        return path.contains("[any]");
    }

    private boolean isPathTokenWithAnyQuantifier(String pathToken) {
        return pathToken.endsWith("[any]");
    }

    static int getArrayCellPosition(PortablePosition arrayPosition, int index, BufferObjectDataInput in)
            throws IOException {
        return in.readInt(arrayPosition.getStreamPosition() + index * Bits.INT_SIZE_IN_BYTES);
    }

    private HazelcastSerializationException unknownFieldException(String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    private IllegalArgumentException wrongUseOfAnyOperationException(String fieldName) {
        return new IllegalArgumentException("Wrong use of any operator: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    private void validateArrayType(FieldDefinition fd, String fieldName) {
        if (!fd.getType().isArrayType()) {
            throw new IllegalArgumentException("Wrong use of array operator: '" + fieldName
                    + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
        }
    }

    private static class NavigationFrame {
        final ClassDefinition cd;

        final int pathTokenIndex;
        final int arrayIndex;

        final int streamPosition;
        final int streamOffset;

        NavigationFrame(ClassDefinition cd, int pathTokenIndex, int arrayIndex, int streamPosition,
                        int streamOffset) {
            this.cd = cd;
            this.pathTokenIndex = pathTokenIndex;
            this.arrayIndex = arrayIndex;
            this.streamPosition = streamPosition;
            this.streamOffset = streamOffset;
        }
    }

}
