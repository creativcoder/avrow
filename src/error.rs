#![allow(missing_docs)]

use serde::{de, ser};
use std::fmt::Debug;
use std::fmt::Display;
use std::io::{Error, ErrorKind};

#[inline(always)]
pub(crate) fn io_err(msg: &str) -> Error {
    Error::new(ErrorKind::Other, msg)
}

// Required impls for Serde
impl ser::Error for AvrowErr {
    fn custom<T: Display>(msg: T) -> Self {
        Self::Message(msg.to_string())
    }
}

impl de::Error for AvrowErr {
    fn custom<T: Display>(msg: T) -> Self {
        Self::Message(msg.to_string())
    }
}

pub type AvrowResult<T> = Result<T, AvrowErr>;

/// Errors returned from avrow
#[derive(thiserror::Error, Debug)]
pub enum AvrowErr {
    // Encode errors
    #[error("Write failed")]
    EncodeFailed(#[source] std::io::Error),
    #[error("Encoding failed. Value does not match schema")]
    SchemaDataMismatch,
    #[error("Expected magic header: `Obj\n`")]
    InvalidDataFile,
    #[error("Sync marker does not match as expected")]
    SyncMarkerMismatch,
    #[error("Named schema not found in union")]
    SchemaNotFoundInUnion,
    #[error("Invalid field value: {0}")]
    InvalidFieldValue(String),
    #[error("Writer seek failed, not a valid avro data file")]
    WriterSeekFailed,
    #[error("Unions must not contain immediate union values")]
    NoImmediateUnion,
    #[error("Failed building the Writer")]
    WriterBuildFailed,
    #[error("Json must be an object for record")]
    ExpectedJsonObject,

    // Decode errors
    #[error("Read failed")]
    DecodeFailed(#[source] std::io::Error),
    #[error("failed reading `avro.schema` metadata from header")]
    HeaderDecodeFailed,
    #[error("Unsupported codec {0}, did you enable the feature?")]
    UnsupportedCodec(String),
    #[error("Named schema was not found in schema registry")]
    NamedSchemaNotFound,
    #[error("Schema resolution failed. reader's schema {0} != writer's schema {1}")]
    SchemaResolutionFailed(String, String),
    #[error("Index read for enum is out of range as per schema. got: {0} symbols: {1}")]
    InvalidEnumSymbolIdx(usize, String),
    #[error("Field not found in record")]
    FieldNotFound,
    #[error("Writer schema not found in reader's schema")]
    WriterNotInReader,
    #[error("Reader's union schema does not match with writer's union schema")]
    UnionSchemaMismatch,
    #[error("Map's value schema do not match")]
    MapSchemaMismatch,
    #[error("Fixed schema names do not match")]
    FixedSchemaNameMismatch,
    #[error("Could not find symbol at index {idx} in reader schema")]
    EnumSymbolNotFound { idx: usize },
    #[error("Reader's enum name does not match writer's enum name")]
    EnumNameMismatch,
    #[error("Readers' record name does not match writer's record name")]
    RecordNameMismatch,
    #[error("Array items schema does not match")]
    ArrayItemsMismatch,
    #[error("Snappy decoder failed to get length of decompressed buffer")]
    SnappyDecompressLenFailed,
    #[error("End of file reached")]
    Eof,

    // Schema parse errors
    #[error("Failed to parse avro schema")]
    SchemaParseErr(#[source] std::io::Error),
    #[error("Unknown schema, expecting a required `type` field in schema")]
    SchemaParseFailed,
    #[error("Expecting fields key as a json array, found: {0}")]
    SchemaFieldParseErr(String),
    #[error("Expected: {0}, found: {1}")]
    SchemaDataValidationFailed(String, String),
    #[error("Schema has a field not found in the value")]
    RecordFieldMissing,
    #[error("Record schema does not a have a required field named `name`")]
    RecordNameNotFound,
    #[error("Record schema does not a have a required field named `type`")]
    RecordTypeNotFound,
    #[error("Expected record field to be a json array")]
    ExpectedFieldsJsonArray,
    #[error("Record's field json schema must be an object")]
    InvalidRecordFieldType,
    #[error("{0}")]
    ParseFieldOrderErr(String),
    #[error("Could not parse name from json value")]
    NameParseFailed,
    #[error("Parsing canonical form failed")]
    ParsingCanonicalForm,
    #[error("Duplicate definition of named schema")]
    DuplicateSchema,
    #[error("Duplicate field name in record schema")]
    DuplicateField,
    #[error("Invalid default value for union. Must be the first entry from union definition")]
    FailedDefaultUnion,
    #[error("Invalid default value for given schema")]
    DefaultValueParse,
    #[error("Unknown field ordering value.")]
    UnknownFieldOrdering,
    #[error("Field ordering value must be a string")]
    InvalidFieldOrdering,
    #[error("Failed to parse symbol from enum's symbols field")]
    EnumSymbolParseErr,
    #[error("Enum schema must contain required `symbols` field")]
    EnumSymbolsMissing,
    #[error("Enum value symbol not present in enum schema `symbols` field")]
    EnumSymbolNotPresent,
    #[error("Fixed schema `size` field must be a number")]
    FixedSizeNotNumber,
    #[error("Fixed schema `size` field missing")]
    FixedSizeNotFound,
    #[error("Unions cannot have multiple schemas of same type or immediate unions")]
    DuplicateSchemaInUnion,
    #[error("Expected the avro schema to be as one of json string, object or an array")]
    UnknownSchema,
    #[error("Expected record field to be a json object, found {0}")]
    InvalidSchema(String),
    #[error("{0}")]
    InvalidDefaultValue(String),
    #[error("Invalid type for {0}")]
    InvalidType(String),
    #[error("Enum schema parsing failed, found: {0}")]
    EnumParseErr(String),
    #[error("Primitve schema must be a string")]
    InvalidPrimitiveSchema,

    // Validation errors
    #[error("Mismatch in fixed bytes length: {found}, {expected}")]
    FixedValueLenMismatch { found: usize, expected: usize },
    #[error("namespaces must either be empty or follow the grammer <name>[(<dot><name>)*")]
    InvalidNamespace,
    #[error("Field name must be [A-Za-z_] and subsequently contain only [A-Za-z0-9_]")]
    InvalidName,
    #[error("Array value is empty")]
    EmptyArray,
    #[error("Map value is empty")]
    EmptyMap,
    #[error("Crc generation failed")]
    CRCGenFailed,
    #[error("Snappy Crc mismatch")]
    CRCMismatch { found: u32, expected: u32 },
    #[error("Named schema was not found for given value")]
    NamedSchemaNotFoundForValue,
    #[error("Value schema not found in union")]
    NotFoundInUnion,

    // Serde specific errors
    #[error("Serde error: {0}")]
    Message(String),
    #[error("Syntax error occured")]
    Syntax,
    #[error("Expected a string value")]
    ExpectedString,
    #[error("Unsupported type")]
    Unsupported,
    #[error("Unexpected avro value: {value}")]
    UnexpectedAvroValue { value: String },

    // Value errors
    #[error("Expected value not found in variant instance")]
    ExpectedVariantNotFound,
}
