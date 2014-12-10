/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.mappable;

import java.util.Map;
import org.elasticlib.common.value.Value;

/**
 * Represents a type that can be converted to a map of values.
 */
public interface Mappable {

    /**
     * Writes this instance to a map of values.
     *
     * @return A map of values.
     */
    Map<String, Value> toMap();
}
