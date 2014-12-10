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
package org.elasticlib.common.client;

import java.io.IOException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

/**
 * Ensure headers are not modified by any MessageBodyWriter.
 * <p>
 * Header modification is not supported by current transport connector (Apache HTTP). Therefore, without this
 * interceptor, warnings are raised because MultipartWriter attempts to set mime-version header.
 */
public class HeaderRestoringWriterInterceptor implements WriterInterceptor {

    /**
     * Constructor.
     */
    HeaderRestoringWriterInterceptor() {
        // This class is an implementation detail.
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException {
        MultivaluedMap<String, Object> before = context.getHeaders();
        context.proceed();
        MultivaluedMap<String, Object> after = context.getHeaders();
        after.clear();
        before.entrySet().forEach(entry -> {
            after.addAll(entry.getKey(), entry.getValue());
        });
    }
}
