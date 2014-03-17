/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hoya.core.conf;

import org.apache.hoya.core.CoreKeys;
import org.apache.hoya.core.persist.JsonSerDeser;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.tools.HoyaUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConfTreeOperations {

  public final ConfTree confTree;
  private final MapOperations globalOptions;


  public ConfTreeOperations(ConfTree confTree) {
    assert confTree != null : "null tree";
    assert confTree.components != null : "null tree components";
    this.confTree = confTree;
    globalOptions = new MapOperations(confTree.global);
  }

  /**
   * Get the underlying conf tree
   * @return the tree
   */
  public ConfTree getConfTree() {
    return confTree;
  }

  /**
   * Validate the configuration
   * @throws BadConfigException
   */
  public void validate() throws BadConfigException {
    String version = confTree.schema;
    if (version == null) {
      throw new BadConfigException("'version' undefined");
    }
    if (!CoreKeys.SCHEMA.equals(version)) {
      throw new BadConfigException(
        "version %s incompatible with supported version %s",
        version,
        CoreKeys.SCHEMA);
    }
  }

  /**
   * Resolve a ConfTree by mapping all global options into each component
   * -if there is none there already
   */
  public void resolve() {
    for (Map.Entry<String, Map<String, String>> comp : confTree.components.entrySet()) {
      mergeInGlobal(comp.getValue());
    }
  }

  /**
   * Merge any options
   * @param component dest values
   */
  public void mergeInGlobal(Map<String, String> component) {
    HoyaUtils.mergeMapsIgnoreDuplicateKeys(component, confTree.global);
  }

  /**
   * Get operations on the global set
   * @return a wrapped map
   */
  public MapOperations getGlobalOptions() {
    return globalOptions;
  }


  /**
   * look up a component and return its options
   * @param component component name
   * @return component mapping or null
   */
  public MapOperations getComponentOperations(String component) {
    Map<String, String> instance = confTree.components.get(component);
    if (instance != null) {
      return new MapOperations(instance);
    }
    return null;
  }

  /**
   * Get a component -adding it to the components map if
   * none with that name exists
   * @param name role
   * @return role mapping
   */
  public MapOperations getOrAddComponent(String name) {
    MapOperations operations = getComponentOperations(name);
    if (operations != null) {
      return operations;
    }
    //create a new instances
    Map<String, String> map = new HashMap<String, String>();
    confTree.components.put(name, map);
    return new MapOperations(map);
  }


  /*
   * return the Set of names names
   */
  @JsonIgnore
  public Set<String> getComponentNames() {
    return new HashSet<String>(confTree.components.keySet());
  }

  /**
   * Get a component whose presence is mandatory
   * @param name component name
   * @return the mapping
   * @throws BadConfigException if the name is not there
   */
  public MapOperations getMandatoryComponent(String name) throws
                                                          BadConfigException {
    MapOperations ops = getComponentOperations(name);
    if (ops == null) {
      throw new BadConfigException("Missing component " + name);
    }
    return ops;
  }

  /**
   * Set a global option, converting it to a string as needed
   * @param key key
   * @param value non null value
   */
  public void set(String key, Object value) {
    globalOptions.put(key, value.toString());
  }

  /**
   * Propagate all global keys matching a prefix
   * @param src source
   * @param prefix prefix
   */
  public void propagateGlobalKeys(ConfTree src, String prefix) {
    Map<String, String> global = src.global;
    for (Map.Entry<String, String> entry : global.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        set(key, entry.getValue());
      }
    }
  }

  /**
   * Merge the map of a single component
   * @param component component name
   * @param map map to merge
   */
  public void mergeSingleComponentMap(String component, Map<String, String> map) {
    MapOperations comp = getOrAddComponent(component);
    comp.putAll(map);
  }
  /**
   * Merge the map of a single component
   * @param component component name
   * @param map map to merge
   */
  public void mergeSingleComponentMapPrefix(String component,
                                            Map<String, String> map,
                                            String prefix,
                                            boolean overwrite) {
    MapOperations comp = getOrAddComponent(component);
    comp.mergeMapPrefixedKeys(map,prefix, overwrite);
  }

  /**
   * Merge in components
   * @param commandOptions component options on the CLI
   */
  public void mergeComponents(Map<String, Map<String, String>> commandOptions) {
    for (Map.Entry<String, Map<String, String>> entry : commandOptions.entrySet()) {
      mergeSingleComponentMap(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Merge in components
   * @param commandOptions component options on the CLI
   */
  public void mergeComponentsPrefix(Map<String,
    Map<String, String>> commandOptions,
                                    String prefix,
                                    boolean overwrite) {
    for (Map.Entry<String, Map<String, String>> entry : commandOptions.entrySet()) {
      mergeSingleComponentMapPrefix(entry.getKey(), entry.getValue(), prefix, overwrite);
    }
  }

  /**
   * Merge in another tree -no overwrites of global or conf data
   * (note that metadata does a naive putAll merge/overwrite)
   * @param that the other tree
   */
  public void mergeWithoutOverwrite(ConfTree that) {

    getGlobalOptions().mergeWithoutOverwrite(that.global);
    confTree.metadata.putAll(that.metadata);

    for (Map.Entry<String, Map<String, String>> entry : that.components.entrySet()) {
      MapOperations comp = getOrAddComponent(entry.getKey());
      comp.mergeWithoutOverwrite(entry.getValue());
    }
  }
  
  /**
   * Merge in another tree with overwrites
   * @param that the other tree
   */
  public void putAll(ConfTree that) {

    getGlobalOptions().putAll(that.global);
    confTree.metadata.putAll(that.metadata);
    
    for (Map.Entry<String, Map<String, String>> entry : that.components.entrySet()) {
      MapOperations comp = getOrAddComponent(entry.getKey());
      comp.putAll(entry.getValue());
    }
  }
  
  /**
   * Load from a resource. The inner conf tree is the loaded data -unresolved
   * @param resource resource
   * @return loaded value
   * @throws IOException load failure
   */
  public static ConfTreeOperations fromResource(String resource) throws
                                                                 IOException {
    JsonSerDeser<ConfTree> confTreeSerDeser =
      new JsonSerDeser<ConfTree>(ConfTree.class);
    ConfTreeOperations ops = new ConfTreeOperations(
       confTreeSerDeser.fromResource(resource) );
    return ops;      
  }
  /**
   * Load from a resource. The inner conf tree is the loaded data -unresolved
   * @param resource resource
   * @return loaded value
   * @throws IOException load failure
   */
  public static ConfTreeOperations fromFile(File resource) throws
                                                                 IOException {
    JsonSerDeser<ConfTree> confTreeSerDeser =
      new JsonSerDeser<ConfTree>(ConfTree.class);
    ConfTreeOperations ops = new ConfTreeOperations(
       confTreeSerDeser.fromFile(resource) );
    return ops;      
  }

  
}
