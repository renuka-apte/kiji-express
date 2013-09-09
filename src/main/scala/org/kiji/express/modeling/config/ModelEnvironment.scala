/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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
 */

package org.kiji.express.modeling.config

import scala.io.Source

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.avro.AvroModelEnvironment
import org.kiji.express.util.Resources.doAndClose
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.util.FromJson
import org.kiji.schema.util.KijiNameValidator
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson
import org.kiji.express.modeling.framework.ModelConverters

/**
 * A ModelEnvironment is a specification describing how to execute a linked model definition.
 * This includes specifying:
 * <ul>
 *   <li>A model definition to run</li>
 *   <li>Mappings from logical names of data sources to their physical realizations</li>
 * </ul>
 *
 * A ModelEnvironment can be created programmatically:
 * {{{
 *   val modelEnv = ModelEnvironment(
 *       name = "myname",
 *       version = "1.0.0",
 *       prepareEnvironment = None,
 *       trainEnvironment = None,
 *       scoreEnvironment = Some(ScoreEnvironment(
 *           inputSpec = KijiInputSpec(
 *               tableUri = "kiji://.env/default/mytable/",
 *               dataRequest = ExpressDataRequest(
 *                   minTimestamp = 0L
 *                   maxTimestamp = 38475687L
 *                   columnRequests = Seq(
 *                       ExpressColumnRequest(
 *                           name = "info:in",
 *                           maxVersions = 3,
 *                           filter = None))),
 *               Seq(FieldBinding("tuplename", "info:storefieldname"))),
 *           outputSpec = KijiSingleColumnOutputSpec(
 *               tableUri = "kiji://.env/default/mytable/",
 *               outputColumn = "outputFamily:qualifier"),
 *           keyValueStoreSpecs = Seq(
 *               KeyValueStore(
 *                   storeType = "KIJI_TABLE",
 *                   name = "myname",
 *                   properties = Map(
 *                       "uri" -> "kiji://.env/default/table",
 *                       "column" -> "info:email")),
 *               KeyValueStore(
 *                   storeType = "AVRO_KV",
 *                   name = "storename",
 *                   properties = Map(
 *                       "path" -> "/some/great/path"))))))
 * }}}
 *
 * Alternatively a ModelEnvironment can be created from JSON. JSON run specifications should
 * be written using the following format:
 * {{{
 * {
 *  "protocol_version":"model_environment-0.2.0",
 *  "name":"myRunProfile",
 *  "version":"1.0.0",
 *  "prepare_environment":{
 *    "org.kiji.express.avro.AvroPrepareEnvironment":{
 *      "input_config":{
 *        "specification":{
 *          "org.kiji.express.avro.AvroKijiInputSpec":{
 *            "table_uri":"kiji://.env/default/table",
 *            "data_request":{
 *              "min_timestamp":0,
 *              "max_timestamp":38475687,
 *              "column_definitions":[{
 *                "name":"info:in",
 *                "max_versions":3,
 *                "filter":{
 *                  "org.kiji.express.avro.RegexQualifierFilterSpec":{
 *                    "regex":"foo"
 *                  }
 *                }
 *              }]
 *            },
 *            "field_bindings":[{
 *              "tuple_field_name":"tuplename",
 *              "store_field_name":"info:storefieldname"
 *            }]
 *          }
 *        }
 *      },
 *      "output_config":{
 *        "specification":{
 *          "org.kiji.express.avro.AvroKijiOutputSpec":{
 *            "table_uri":"kiji://myuri",
 *            "field_bindings":[{
 *              "tuple_field_name":"outputtuple",
 *              "store_field_name":"info:out"
 *            }]
 *          }
 *        }
 *      },
 *      "kv_stores":[{
 *        "store_type":"AVRO_KV",
 *        "name":"side_data",
 *        "properties":[{
 *          "name":"path",
 *          "value":"/usr/src/and/so/on"
 *        }]
 *      }]
 *    }
 *  }
 * }
 * }}}
 *
 * To load a JSON model environment:
 * {{{
 * // Load a JSON string directly:
 * val myModelEnvironment: ModelEnvironment =
 *     ModelEnvironment.loadJson("""{ "name": "myIdentifier", ... }""")
 *
 * // Load a JSON file:
 * val myModelEnvironment2: ModelEnvironment =
 *     ModelEnvironment.loadJsonFile("/path/to/json/config.json")
 * }}}
 *
 * @param name of the model environment.
 * @param version of the model environment.
 * @param prepareEnvironment defining configuration details specific to the Prepare phase of a
 *     model. Optional.
 * @param trainEnvironment defining configuration details specific to the Train phase of a model.
 *     Optional.
 * @param scoreEnvironment defining configuration details specific to the Score phase of a model.
 *     Optional.
 * @param protocolVersion this model definition was written for.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ModelEnvironment(
    name: String,
    version: String,
    prepareEnvironment: Option[PrepareEnvironment] = None,
    trainEnvironment: Option[TrainEnvironment] = None,
    scoreEnvironment: Option[ScoreEnvironment] = None,
    private[express] val protocolVersion: ProtocolVersion =
        ModelEnvironment.CURRENT_MODEL_DEF_VER) {
  // Ensure that all fields set for this model environment are valid.
  ModelEnvironment.validateModelEnvironment(this)

  /**
   * Serializes this model environment into a JSON string.
   *
   * @return a JSON string that represents this model environment.
   */
  def toJson: String = {
    val environment: AvroModelEnvironment = ModelConverters.modelEnvironmentToAvro(this)

    ToJson.toAvroJsonString(environment)
  }

  /**
   * Creates a new model environment with settings specified to this method. Any setting specified
   * to this method is used in the new model environment. Any unspecified setting will use the
   * value from this model environment in the new model environment.
   *
   * @param name of the model environment.
   * @param version of the model environment.
   * @param prepareEnvironment defining configuration details specific to the Prepare phase of a
   *     model.
   * @param trainEnvironment defining configuration details specific to the Train phase of a model.
   * @param scoreEnvironment defining configuration details specific to the Score phase of a model.
   */
  def withNewSettings(
      name: String = this.name,
      version: String = this.version,
      prepareEnvironment: Option[PrepareEnvironment] = this.prepareEnvironment,
      trainEnvironment: Option[TrainEnvironment] = this.trainEnvironment,
      scoreEnvironment: Option[ScoreEnvironment] = this.scoreEnvironment): ModelEnvironment = {
    new ModelEnvironment(
        name,
        version,
        prepareEnvironment,
        trainEnvironment,
        scoreEnvironment)
  }
}

/**
 * Companion object for ModelEnvironment. Contains constants related to model environments as well
 * as validation methods.
 */
object ModelEnvironment {
  /** Maximum model environment version we can recognize. */
  val MAX_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.2.0")

  /** Minimum model environment version we can recognize. */
  val MIN_RUN_ENV_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.2.0")

  /** Current ModelDefinition protocol version. */
  val CURRENT_MODEL_DEF_VER: ProtocolVersion = ProtocolVersion.parse("model_environment-0.2.0")

  /** Regular expression used to validate a model environment version string. */
  val VERSION_REGEX: String = "[0-9]+(.[0-9]+)*"

  /** Message to show the user when there is an error validating their model definition. */
  private[express] val VALIDATION_MESSAGE = "One or more errors occurred while validating your" +
      " model environment. Please correct the problems in your model environment and try again."

  /**
   * Creates a ModelEnvironment given a JSON string. In the process, all fields are validated.
   *
   * @param json serialized model environment.
   * @return the validated model environment.
   */
  def fromJson(json: String): ModelEnvironment = {
    // Parse the json.
    val avroModelEnvironment: AvroModelEnvironment = FromJson
        .fromJsonString(json, AvroModelEnvironment.SCHEMA$)
        .asInstanceOf[AvroModelEnvironment]

    // Build a model environment.
    ModelConverters.modelEnvironmentFromAvro(avroModelEnvironment)
  }

  /**
   * Creates an ModelEnvironment given a path in the local filesystem to a JSON file that specifies
   * a run specification. In the process, all fields are validated.
   *
   * @param path in the local filesystem to a JSON file containing a model environment.
   * @return the validated model environment.
   */
  def fromJsonFile(path: String): ModelEnvironment = {
    val json: String = doAndClose(Source.fromFile(path)) { source => source.mkString }

    fromJson(json)
  }

  /**
   * Verifies that all fields in a model environment are valid. This validation method will collect
   * all validation errors into one exception.
   *
   * @param environment to validate.
   * @throws a ModelEnvironmentValidationException if there are errors encountered while validating
   *     the provided model definition.
   */
  def validateModelEnvironment(environment: ModelEnvironment) {
    // Collect errors from other validation steps.
    val baseErrors: Seq[ValidationException] =
        validateProtocolVersion(environment.protocolVersion).toSeq ++
        validateName(environment.name) ++
        validateVersion(environment.version)

    val prepareErrors: Seq[ValidationException] = environment
        .prepareEnvironment
        .map { env => validatePrepareEnv(env) }
        .getOrElse(Seq())
    val trainErrors: Seq[ValidationException] = environment
        .trainEnvironment
        .map { env => validateTrainEnv(env) }
        .getOrElse(Seq())
    val scoreErrors: Seq[ValidationException] = environment
        .scoreEnvironment
        .map { env => validateScoreEnv(env) }
        .getOrElse(Seq())

    // Throw an exception if there were any validation errors.
    val causes = baseErrors ++ prepareErrors ++ trainErrors ++ scoreErrors
    if (!causes.isEmpty) {
      throw new ModelEnvironmentValidationException(causes)
    }
  }

  /**
   * Verifies that a model environment's protocol version is supported.
   *
   * @param protocolVersion to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     protocol version.
   */
  def validateProtocolVersion(protocolVersion: ProtocolVersion): Option[ValidationException] = {
    if (MAX_RUN_ENV_VER.compareTo(protocolVersion) < 0) {
      val error = "\"%s\" is the maximum protocol version supported. ".format(MAX_RUN_ENV_VER) +
          "The provided model environment is of protocol version: \"%s\"".format(protocolVersion)
      Some(new ValidationException(error))
    } else if (MIN_RUN_ENV_VER.compareTo(protocolVersion) > 0) {
      val error = "\"%s\" is the minimum protocol version supported. ".format(MIN_RUN_ENV_VER) +
          "The provided model environment is of protocol version: \"%s\"".format(protocolVersion)
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Verifies that a model environment's name is valid.
   *
   * @param name to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     name of the model environment.
   */
  def validateName(name: String): Option[ValidationException] = {
    if(name.isEmpty) {
      val error = "The name of the model environment cannot be the empty string."
      Some(new ValidationException(error))
    } else if (!KijiNameValidator.isValidAlias(name)) {
      val error = "The name \"%s\" is not valid. Names must match the regex \"%s\"."
          .format(name, KijiNameValidator.VALID_ALIAS_PATTERN.pattern)
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Verifies that a model environment's version string is valid.
   *
   * @param version to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     version string.
   */
  def validateVersion(version: String): Option[ValidationException] = {
    if (!version.matches(VERSION_REGEX)) {
      val error = "Model environment version strings must match the regex " +
          "\"%s\" (1.0.0 would be valid).".format(VERSION_REGEX)
      Some(new ValidationException(error))
    }
    else {
      None
    }
  }

  /**
   * Verifies that the given sequence of FieldBindings is valid with respect to the field names and
   * column names contained therein.
   *
   * @param fieldBindings are the associations between field names and Kiji column names.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateKijiInputOutputFieldBindings(
      fieldBindings: Seq[FieldBinding]): Seq[ValidationException] = {
    // Validate column names.
    val columnNames: Seq[String] = fieldBindings
        .map { fieldBinding: FieldBinding => fieldBinding.storeFieldName }
    val columnNameErrors = columnNames
        .map { columnName: String => validateKijiColumnName(columnName) }
        .flatten

    // Validate field name bindings.
    val fieldNames: Seq[String] = fieldBindings
        .map { fieldBinding: FieldBinding => fieldBinding.tupleFieldName }
    val fieldNameErrors = validateFieldNames(fieldNames).toSeq ++ validateColumnNames(columnNames)

    columnNameErrors ++ fieldNameErrors
  }

  /**
   * Verifies that the InputSpec of the train or prepare phase is valid.
   *
   * @param inputSpec to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateInputSpec(inputSpec: InputSpec): Seq[ValidationException] = {
    inputSpec match {
      case kijiInputSpec: KijiInputSpec => {
        val fieldBindingErrors = validateKijiInputOutputFieldBindings(kijiInputSpec.fieldBindings)
        val dataRequestErrors = validateDataRequest(kijiInputSpec.dataRequest)

        fieldBindingErrors ++ dataRequestErrors
      }
      // TODO(EXP-161): Accept inputs from multiple source types.
      case _ => {
        val error = "Unsupported InputSpec type: %s".format(inputSpec.getClass)

        Seq(new ValidationException(error))
      }
    }
  }

  /**
   * Verifies that the OutputSpec of the train or prepare phase is valid.
   *
   * @param outputSpec to validate.
   * @return an optional ValidationException if there are errors encountered.
   */
  def validateOutputSpec(outputSpec: OutputSpec): Seq[ValidationException] = {
    outputSpec match {
      case kijiOutputSpec: KijiOutputSpec => {
        validateKijiInputOutputFieldBindings(kijiOutputSpec.fieldBindings)
      }
      case kijiSingleColumnSpec: KijiSingleColumnOutputSpec => {
        validateKijiColumnName(kijiSingleColumnSpec.outputColumn)
            .toSeq
      }
      // TODO(EXP-161): Accept outputs from multiple source types.
      case _ => {
        val error = "Unsupported OutputSpec type: %s".format(outputSpec.getClass)

        Seq(new ValidationException(error))
      }
    }
  }

  /**
   * Verifies that a model environment's prepare phase is valid.
   *
   * @param prepareEnvironment to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     prepare phase.
   */
  def validatePrepareEnv(prepareEnvironment: PrepareEnvironment): Seq[ValidationException] = {
    val inputSpecErrors = validateInputSpec(prepareEnvironment.inputSpec)
    val outputSpecErrors = validateOutputSpec(prepareEnvironment.outputSpec)
    val kvStoreErrors = validateKvStores(prepareEnvironment.keyValueStoreSpecs)

    inputSpecErrors ++ outputSpecErrors ++ kvStoreErrors
  }

  /**
   * Verifies that a model environment's train phase is valid.
   *
   * @param trainEnvironment to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     prepare phase.
   */
  def validateTrainEnv(trainEnvironment: TrainEnvironment): Seq[ValidationException] = {
    val inputSpecErrors = validateInputSpec(trainEnvironment.inputSpec)
    val outputSpecErrors = validateOutputSpec(trainEnvironment.outputSpec)
    val kvStoreErrors = validateKvStores(trainEnvironment.keyValueStoreSpecs)

    inputSpecErrors ++ outputSpecErrors ++ kvStoreErrors
  }

  /**
   * Verifies that a model environment's score phase is valid.
   *
   * @param scoreEnvironment to validate.
   * @return an optional ValidationException if there are errors encountered while validating the
   *     score phase.
   */
  def validateScoreEnv(scoreEnvironment: ScoreEnvironment): Seq[ValidationException] = {
    val inputSpecErrors = validateInputSpec(scoreEnvironment.inputSpec)
    // The score phase only supports the single column Kiji output.
    val outputSpecErrors = scoreEnvironment.outputSpec match {
      case scorePhaseOutputSpec: KijiSingleColumnOutputSpec => {
        validateKijiColumnName(scorePhaseOutputSpec.outputColumn)
            .toSeq
      }
      case _ => {
        val error = "Unsupported OutputSpec type for Score Phase: %s"
            .format(scoreEnvironment.outputSpec.getClass)

        Seq(new ValidationException(error))
      }
    }
    val kvStoreErrors = validateKvStores(scoreEnvironment.keyValueStoreSpecs)

    inputSpecErrors ++ outputSpecErrors ++ kvStoreErrors
  }

  /**
   * Validates a data request.
   *
   * @param dataRequest to validate.
   * @throws a ModelEnvironmentValidationException if the column names are invalid.
   */
  def validateDataRequest(dataRequest: ExpressDataRequest): Seq[ValidationException] = {
    // Validate Kiji column names.
    val kijiColumnNameErrors: Seq[ValidationException] = dataRequest
        .columnRequests
        .map { column: ExpressColumnRequest => validateKijiColumnName(column.name) }
        .flatten

    // Validate the minimum timestamp.
    val minimumTimestampError: Option[ValidationException] =
        if (dataRequest.minTimestamp < 0) {
          val error = "minTimestamp in the DataRequest is " + dataRequest.minTimestamp +
              " and must be greater than 0"
          Some(new ValidationException(error))
        } else {
          None
        }

    // Validate the maximum timestamp.
    val maximumTimestampError: Option[ValidationException] =
        if (dataRequest.maxTimestamp < 0) {
          val error = "maxTimestamp in the DataRequest is " + dataRequest.minTimestamp +
              " and must be greater than 0"
          Some(new ValidationException(error))
        } else {
          None
        }

    kijiColumnNameErrors ++ minimumTimestampError ++ maximumTimestampError
  }

  /**
   * Validate the name of a KVStore.
   *
   * @param keyValueStoreSpec whose name should be validated.
   * @return an optional ValidationException if the name provided is invalid.
   */
  def validateKvstoreName(keyValueStoreSpec: KeyValueStoreSpec): Option[ValidationException] = {
    if (!keyValueStoreSpec.name.matches("^[a-zA-Z_][a-zA-Z0-9_]+$")) {
      val error = "The key-value store name must begin with a letter" +
          " and contain only alphanumeric characters and underscores. The key-value store" +
          " name you provided is " + keyValueStoreSpec.name + " and the regex it must match is " +
          "^[a-zA-Z_][a-zA-Z0-9_]+$"
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validate properties specified to initialize the key-value store. Each type of key-value store
   * requires different properties.
   *
   * @param keyValueStoreSpec whose properties to validate.
   * @return an optional ValidationException if any of the properties are invalid.
   */
  def validateKvstoreProperties(
      keyValueStoreSpec: KeyValueStoreSpec): Option[ValidationException] = {
    val properties: Map[String, String] = keyValueStoreSpec.properties

    keyValueStoreSpec.storeType match {
      case "AVRO_KV" => {
        if (!properties.contains("path")) {
          val error = "To use an Avro key-value record key-value store, you must specify the" +
              " HDFS path to the Avro container file to use to back the store. Use the" +
              " property name 'path' to provide the path."
          return Some(new ValidationException(error))
        }
      }

      case "AVRO_RECORD" => {
        // Construct an error message for a missing path, missing key_field, or both.
        val pathError =
          if (!properties.contains("path")) {
            "To use an Avro record key-value store, you must specify the HDFS path" +
                " to the Avro container file to use to back the store. Use the property name" +
                " 'path' to provide the path."
          } else {
            ""
          }

        val keyFieldError =
          if (!properties.contains("key_field")) {
            "To use an Avro record key-value store, you must specify the name" +
                " of a record field whose value should be used as the record's key. Use the" +
                " property name 'key_field' to provide the field name."
          } else {
            ""
          }

        val errorMessage = pathError + keyFieldError
        return Some(new ValidationException(errorMessage))
      }

      case "KIJI_TABLE" => {
        if (!properties.contains("uri")) {
          val error = "To use a Kiji table key-value store, you must specify a Kiji URI" +
              " addressing the table to use to back the store. Use the property name 'url' to" +
              " provide the URI."
          return Some(new ValidationException(error))
        }
        if (!properties.contains("column")) {
          val error = "To use a Kiji table key-value store, you must specify the qualified-name" +
              " of the column whose most recent value will be used as the value associated with" +
              " each entity id. Use the property name 'column' to specify the column name."
          return Some(new ValidationException(error))
        }
      }

      case keyValueStoreType => {
        val error = "An unknown key-value store type was specified: " + keyValueStoreType
        return Some(new ValidationException(error))
      }
    }

    return None
  }

  /**
   * Validates all properties of Seq of KVStores.
   *
   * @param keyValueStoreSpecs to validate
   * @return validation exceptions generated while validate the KVStore specification.
   */
  def validateKvStores(keyValueStoreSpecs: Seq[KeyValueStoreSpec]): Seq[ValidationException] = {
    // Collect KVStore errors.
    val nameErrors: Seq[ValidationException] = keyValueStoreSpecs
        .map { keyValueStore: KeyValueStoreSpec => validateKvstoreName(keyValueStore) }
        .flatten
    val propertiesErrors: Seq[ValidationException] = keyValueStoreSpecs
        .map { keyValueStore: KeyValueStoreSpec => validateKvstoreProperties(keyValueStore) }
        .flatten

    nameErrors ++ propertiesErrors
  }

  /**
   * Validates that the tuple field names are distinct and mapped to a unique column name.
   *
   * @param fieldNames to validate.
   * @return an optional ValidationException if the tuple field names are invalid.
   */
  def validateFieldNames(fieldNames: Seq[String]): Option[ValidationException] = {
    if (fieldNames.distinct.size != fieldNames.size) {
      val duplicates = fieldNames.toList.filterNot(x => fieldNames.toList.distinct == x)
      val error = "Every tuple field name must map to a unique column name. You have" +
          " one or more field names that are associated with" +
          " multiple columns: %s.".format(duplicates.mkString(" "))
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validates that the supplied column names are distinct and mapped to a unique tuple field.
   *
   * @param columnNames to validate.
   * @return an optional ValidationException if the column names are invalid.
   */
  def validateColumnNames(columnNames: Seq[String]): Option[ValidationException] = {
    if (columnNames.distinct.size != columnNames.size) {
      val duplicates = columnNames.toList.filterNot(x => columnNames.toList.distinct == x)
      val error = "Every column name must map to a unique tuple field. You have a data " +
          "field associated with multiple columns: %s".format(duplicates.mkString(" "))
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validates the specified output column for the score phase.
   *
   * @param outputColumn to validate.
   * @return an optional ValidationException if the score output column is invalid.
   */
  def validateScoreOutputColumn(outputColumn: String): Option[ValidationException] = {
    if (outputColumn.isEmpty) {
      val error = "The column to write out to in the score phase cannot be the empty string."
      Some(new ValidationException(error))
    } else {
      None
    }
  }

  /**
   * Validates a column name specified in the model environment is can be used to construct a
   * KijiColumnName.
   *
   * @param name is the string representation of the column name.
   * @return an optional ValidationException if the name is invalid.
   */
  def validateKijiColumnName(name: String): Option[ValidationException] = {
    try {
      new KijiColumnName(name)
    } catch {
      case _: KijiInvalidNameException => {
        val error = "The column name " + name + " is invalid."
        return Some(new ValidationException(error))
      }
    }
    None
  }
}
