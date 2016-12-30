/**
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.id;

import lombok.Getter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class IdWithTimestamp {

    @Getter
    private String id;

    public static IdWithTimestamp generate(String name) {
        return new IdWithTimestamp(name);
    }

    private IdWithTimestamp(String name) {
        String normalized_name = name.replaceAll(" ","_");
        id = normalized_name + "-" + getTimestamp();
    }

    private String getTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmmss");
        return dateFormat.format(new Date());
    }
}
