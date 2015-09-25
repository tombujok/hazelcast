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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Arrays;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FieldGetterTest {

    @Test
    public void foo() throws Exception {
        Field limbsField = Body.class.getDeclaredField("limbs");
        Field fingersField = Limb.class.getDeclaredField("fingers");
        Field nailsField = Finger.class.getDeclaredField("nails");
        Field coloursField = Nail.class.getDeclaredField("colours");
        Field fingerNameField = Finger.class.getDeclaredField("name");

        FieldGetter limbGetter = new FieldGetter(null, limbsField, null);
        FieldGetter fingersGetter = new FieldGetter(limbGetter, fingersField, null);
        FieldGetter nailsGetter = new FieldGetter(fingersGetter, nailsField, null);
        FieldGetter coloursGetter = new FieldGetter(nailsGetter, coloursField, null);

        FieldGetter firstFingersGetter = new FieldGetter(limbGetter, fingersField, "(0)");
        FieldGetter fingerNameGetter = new FieldGetter(firstFingersGetter, fingerNameField, null);

        Body body = new Body("cpt. cook",
                new Limb("hook",
                        new Finger("missing",
                                new Nail("ugly",
                                        new String[]{"red", "green"}
                                ),
                                new Nail("nice",
                                        new String[]{"blue", "white"}
                                )
                        ),
                        new Finger("rotten")
                ),
                new Limb("leg",
                        new Finger("left"),
                        new Finger("right")
                )
        );

        Object result = coloursGetter.getValue(body);
        System.out.println(Arrays.toString((Object[]) result));


        result = fingerNameGetter.getValue(body);
        System.out.println(Arrays.toString((Object[]) result));

    }



    private static class Body {
        final String name;
        final Limb[] limbs;

        Body(String name, Limb... limbs) {
            this.name = name;
            this.limbs = limbs;
        }
    }

    private static class Limb {
        final String name;
        final Finger[] fingers;

        Limb(String name, Finger...fingers) {
            this.name = name;
            this.fingers = fingers;
        }
    }

    private static class Finger {
        final String name;
        final Nail[] nails;

        Finger(String name, Nail...nails) {
            this.name = name;
            this.nails = nails;
        }
    }

    private static class Nail {
        final String name;
        final String[] colours;

        Nail(String name, String...colours) {
            this.name = name;
            this.colours = colours;
        }
    }
}
