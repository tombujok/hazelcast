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
public class MapAttributeConfig {

    private String name;
    private String extractor;

    private MapAttributeConfigReadOnly readOnly;

    /**
     * Creates a MapAttributeConfig without an attribute and with ordered set to false.
     */
    public MapAttributeConfig() {
    }

    /**
     * Creates a MapAttributeConfig with the given attribute and ordered setting.
     *
     * @param name the attribute that is going to be indexed.
     * @param extractor      true if the index is ordered.
     * @see #setName(String)
     * @see #setExtractor(String)
     */
    public MapAttributeConfig(String name, String extractor) {
        setName(name);
        setExtractor(extractor);
    }

    public MapAttributeConfig(MapAttributeConfig config) {
        name = config.getName();
        extractor = config.getExtractor();
    }

    public MapAttributeConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapAttributeConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the attribute extracted by the extractor.
     *
     * @return the attribute name to be extracted
     * @see #setName(String)
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name (alias) of the attribute extracted by the extractor.
     *
     * @param name the attribute that is going to be indexed.
     * @return the updated MapAttributeConfig.
     * @throws IllegalArgumentException if attribute is null or an empty string.
     */
    public MapAttributeConfig setName(String name) {
        this.name = checkHasText(name, "Map attribute name must contain text");
        return this;
    }

    public String getExtractor() {
        return extractor;
    }

    public MapAttributeConfig setExtractor(String extractor) {
        this.extractor = checkHasText(extractor, "Map attribute extractor must contain text");
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MapAttributeConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append("extractor='").append(extractor).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
