/*
 * Copyright (C) Red Gate Software Ltd 2010-2021
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Formats execution times.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TimeFormat {
    /**
     * Formats this execution time as minutes:seconds.millis. Ex.: 02:15.123s
     *
     * @param millis The number of millis.
     * @return The execution in a human-readable format.
     */
    public static String format(long millis) {
        return String.format("%02d:%02d.%03ds", millis / 60000, (millis % 60000) / 1000, (millis % 1000));
    }
}