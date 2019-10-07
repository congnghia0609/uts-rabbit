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

import java.io.File;
import java.net.URL;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 *
 * @author nghiatc
 * @since Sep 6, 2015
 */
public class NConfig {
    private static CompositeConfiguration CONFIG_PROPERTIES = null;

	public static final String ENV = System.getProperty("app_env", "development");

	private static Lock lock = new ReentrantLock();
	private static String confDir = System.getProperty("confdir", "conf");

	private NConfig() {
		init();
	}

    static{
        getConfig();
    }
    
	private static void init() {
        System.out.println("==============init NConfig==============");
		String fileName = ENV + ".properties";
		try {
			File file = null;
			if ((confDir != null) && (!confDir.isEmpty())) {
				file = new File(confDir + "/" + fileName);
				System.out.println("Configuration load directory file: " + file.getPath());

				Configuration configProps = loadConfiguration(file);
				CONFIG_PROPERTIES = new CompositeConfiguration();
				CONFIG_PROPERTIES.addConfiguration(configProps);
			} else {
				URL resourceURL = ResourceUtils.getResourceAsURL(fileName);
				System.out.println("Configuration load directory resourceURL: " + resourceURL);

				Configuration configProps = new PropertiesConfiguration();

				if (resourceURL != null) {
					configProps = new PropertiesConfiguration(resourceURL);
				}

				CONFIG_PROPERTIES = new CompositeConfiguration();
				CONFIG_PROPERTIES.addConfiguration(configProps);
			}
		} catch (Exception e) {
			throw new IllegalStateException("ConfigurationException, Unable to load: " + fileName);
		}
	}

	private static Configuration loadConfiguration(File propsFile)
			throws Exception {
		if (propsFile.exists()) {
			return new PropertiesConfiguration(propsFile);
		}
		return new PropertiesConfiguration();
	}

	public static CompositeConfiguration getConfig() {
		if(CONFIG_PROPERTIES == null) {
			lock.lock();
			try {
				if(CONFIG_PROPERTIES == null) {
					NConfig instance = new NConfig();
				}
			} finally {
				lock.unlock();
			}
		}
		return CONFIG_PROPERTIES;
	}

	public static String genKey(String configName, String key) {
		return configName + "." + key;
	}
}
