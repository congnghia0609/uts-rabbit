/*
 * Copyright 2015 nghiatc.
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

package com.uts.configer;

import java.io.InputStream;
import java.net.URL;

/**
 * ResourceUtils. Provide a set of convenience methods for working with
 * Resources.
 * 
 * @author nghiatc
 * @since Sep 6, 2015
 */
public class ResourceUtils {
    /**
     * Get the InputStream for this resource. Note: to convert an InputStream
     * into an InputReader, use: new InputStreamReader(InputStream).
     *
     * @param name The name resource to load.
     *
     * @return If the Resource was found, the InputStream, otherwise null.
     */
    public static InputStream getResourceAsStream(String name) {
        ClassLoader loader = ResourceUtils.class.getClass().getClassLoader();

        InputStream in = null;

        if (loader != null) {
            in = loader.getResourceAsStream(name);
        } else {
            in = ClassLoader.getSystemResourceAsStream(name);
        }

        return in;
    }

    /**
     * Get the URL for this resource.
     *
     * @param name The name resource to load.
     *
     * @return If the Resource was found, the URL, otherwise null.
     */
    public static URL getResourceAsURL(String name) {
        ClassLoader loader = ResourceUtils.class.getClassLoader();

        URL url = null;

        if (loader != null) {
            url = loader.getResource(name);
        } else {
            url = ClassLoader.getSystemResource(name);
        }

        return (url);
    }
}
