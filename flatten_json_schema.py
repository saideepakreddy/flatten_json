from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode_outer, col

# Directory path where JSON files are located
json_directory = "/dbfs/path/to/json/files"

# Flattening function for naming convention
def flatten_schema(schema, parent_name="", level=1):
    fields = []
    for field in schema.fields:
        field_name = f"{parent_name}_{level}_{field.name}" if parent_name else f"{field.name}_{level}"
        if isinstance(field.dataType, StructType):
            fields += flatten_schema(field.dataType, field_name, level + 1)
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            fields += flatten_schema(field.dataType.elementType, field_name, level + 1)
        else:
            fields.append(StructField(field_name, field.dataType, nullable=True))
    return fields

# Merging all schemas
def merge_all_schemas(schemas):
    all_fields = []
    for schema in schemas:
        all_fields += flatten_schema(schema)
    unique_fields = {}
    for field in all_fields:
        if field.name not in unique_fields:
            unique_fields[field.name] = field
    return StructType(list(unique_fields.values()))

# Collect all schemas from each JSON file
file_paths = [file.path for file in dbutils.fs.ls(json_directory) if file.path.endswith(".json")]
schemas = [spark.read.option("multiline", "true").json(file_path).schema for file_path in file_paths]

# Merge schemas into a unified schema
combined_schema = merge_all_schemas(schemas)

# Function to flatten each JSON file using the merged schema
def load_and_flatten_json(file_path, schema):
    df = spark.read.schema(schema).option("multiline", "true").json(file_path)
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType) or (isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType)):
            df = flatten_df(df, field.name)
    return df

def flatten_df(df, column_name):
    while any(isinstance(field.dataType, (StructType, ArrayType)) for field in df.schema.fields):
        df = df.select(*[
            col(f"{column_name}.{sub_field.name}").alias(f"{column_name}_{sub_field.name}") if isinstance(field.dataType, StructType) else
            explode_outer(col(column_name)).alias(column_name) if isinstance(field.dataType, ArrayType) else
            col(column_name)
            for column_name in df.columns
            for field in df.schema.fields
        ])
    return df

# Loop through files, flatten and combine them into a single DataFrame
flattened_dfs = []
for file_path in file_paths:
    try:
        df = load_and_flatten_json(file_path, combined_schema)
        flattened_dfs.append(df)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

# Combine all DataFrames into a single DataFrame
if flattened_dfs:
    combined_df = flattened_dfs[0]
    for df in flattened_dfs[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)
else:
    combined_df = None

# Display the combined DataFrame
if combined_df:
    combined_df.show()
else:
    print("No DataFrames were created successfully.")
