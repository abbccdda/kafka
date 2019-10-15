/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.message;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StructSpecTest {

    /**
     * Test consecutive tags starting from 0.
     */
    @Test
    public void testConsecutiveTags() {
        Set<Integer> tags = new HashSet<>(Arrays.asList(0, 1, 2, 3));
        StructSpec.validateTags("testField", tags);
    }

    /**
     * Test consecutive tags starting from 10000.
     */
    @Test
    public void testConsecutiveTagsOver10000() {
        Set<Integer> tags = new HashSet<>(Arrays.asList(10000, 10001, 10002, 10003));
        StructSpec.validateTags("testField", tags);
    }

    /**
     * Test consecutive tags with both starting with 0 and 10000.
     */
    @Test
    public void testConsecutiveTagsTwoRange() {
        Set<Integer> tags = new HashSet<>(Arrays.asList(0, 1, 2, 10000, 10001, 10002, 10003));
        StructSpec.validateTags("testField", tags);
    }

    /**
     * Test tags with holes starting with 0.
     */
    @Test
    public void testTagsWithHoles() {
        Set<Integer> tags = new HashSet<>(Arrays.asList(0, 1, 3));
        RuntimeException ex = Assert.assertThrows(RuntimeException.class,
                () -> StructSpec.validateTags("testField", tags));
        Assert.assertEquals("In testField, the tag IDs are not contiguous. Make use of tag 2 before using any higher tag IDs.",
                ex.getMessage());
    }

    /**
     * Test tags with holes starting with 10000.
     */
    @Test
    public void testTagsWithHoles10000() {
        Set<Integer> tags = new HashSet<>(Arrays.asList(10000, 10002, 10003));
        RuntimeException ex = Assert.assertThrows(RuntimeException.class,
                () -> StructSpec.validateTags("testField", tags));
        Assert.assertEquals("In testField, the tag IDs are not contiguous. Make use of tag 10001 before using any higher tag IDs.",
                ex.getMessage());
    }

    /**
     * Test tags not starting with 0.
     */
    @Test
    public void testTagsInvalidStartTag() {
        Set<Integer> tags = new HashSet<>(Arrays.asList(2, 3, 4));
        RuntimeException ex = Assert.assertThrows(RuntimeException.class,
                () -> StructSpec.validateTags("testField", tags));
        Assert.assertEquals("In testField, the tag IDs are not contiguous. Make use of tag 0 before using any higher tag IDs.",
                ex.getMessage());
    }

    /**
     * Test tags with holes starting with 10000.
     */
    @Test
    public void testTagsInvalidStartTag10000() {
        Set<Integer> tags = new HashSet<>(Arrays.asList(10004, 10005, 10006));
        RuntimeException ex = Assert.assertThrows(RuntimeException.class,
                () -> StructSpec.validateTags("testField", tags));
        Assert.assertEquals("In testField, the tag IDs are not contiguous. Make use of tag 10000 before using any higher tag IDs.",
                ex.getMessage());
    }
}
