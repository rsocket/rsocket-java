/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.reactivesocket.mimetypes.internal;

import java.util.Map;

public class CustomObject {

    private String name;
    private int age;
    private Map<String, Integer> attributes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Map<String, Integer> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Integer> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        String sb = "CustomObject{" + "name='" + name + '\'' +
                    ", age=" + age +
                    ", attributes=" + attributes +
                    '}';
        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CustomObject)) {
            return false;
        }

        CustomObject that = (CustomObject) o;

        if (age != that.age) {
            return false;
        }
        if (name != null? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (attributes != null? !attributes.equals(that.attributes) : that.attributes != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null? name.hashCode() : 0;
        result = 31 * result + age;
        result = 31 * result + (attributes != null? attributes.hashCode() : 0);
        return result;
    }
}
