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
package org.elasticlib.node.runtime;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Provides various runtime related informations.
 */
public class RuntimeInfo {

    /**
     * @return The application title.
     */
    public String getTitle() {
        return valueOrEmpty(getClass().getPackage().getImplementationTitle());
    }

    /**
     * @return The application version.
     */
    public String getVersion() {
        return valueOrEmpty(getClass().getPackage().getImplementationVersion());
    }

    private static String valueOrEmpty(String value) {
        return value != null ? value : "";
    }

    /**
     * @return The application uptime.
     */
    public Duration getUptime() {
        return Duration.of(ManagementFactory.getRuntimeMXBean().getUptime(), ChronoUnit.MILLIS);
    }
}
