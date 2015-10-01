/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import static com.hazelcast.util.Preconditions.checkHasText;

/**
 * Contains the configuration for an extractor in a map. This class should be used in combination
 * with the {@link MapConfig}. The reason to create an map extractor is to enable custom extraction of values
 * from particular map entries.
 */
public class MapExtractorConfig {

    private String attribute;
    private String type;

    private MapExtractorConfigReadOnly readOnly;

    /**
     * Creates a MapExtractorConfig without an attribute and with ordered set to false.
     */
    public MapExtractorConfig() {
    }

    /**
     * Creates a MapExtractorConfig with the given attribute and ordered setting.
     *
     * @param attribute the attribute that is going to be indexed.
     * @param type      true if the index is ordered.
     * @see #setAttribute(String)
     * @see #setType(String)
     */
    public MapExtractorConfig(String attribute, String type) {
        setAttribute(attribute);
        setType(type);
    }

    public MapExtractorConfig(MapExtractorConfig config) {
        attribute = config.getAttribute();
        type = config.getType();
    }

    public MapExtractorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapExtractorConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name (alias) of the attribute extracted by the extractor.
     *
     * @return the attribute name to be extr.
     * @see #setAttribute(String)
     */
    public String getAttribute() {
        return attribute;
    }

    /**
     * Sets the name (alias) of the attribute extracted by the extractor.
     *
     * @param attribute the attribute that is going to be indexed.
     * @return the updated MapExtractorConfig.
     * @throws IllegalArgumentException if attribute is null or an empty string.
     */
    public MapExtractorConfig setAttribute(String attribute) {
        this.attribute = checkHasText(attribute, "Map extractor attribute must contain text");
        return this;
    }

    public String getType() {
        return type;
    }

    public MapExtractorConfig setType(String type) {
        this.type = checkHasText(type, "Map extractor type must contain text");
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MapExtractorConfig{");
        sb.append("attribute='").append(attribute).append('\'');
        sb.append("type='").append(type).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
