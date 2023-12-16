from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat_ws
from pyspark.ml.feature import NGram, Tokenizer
from pyspark.ml import Pipeline
import re

def main():
    spark = SparkSession.builder.appName("Lab1").getOrCreate()
    json = spark.read.json("10K.github.jsonl")
    push_events = json.filter(col("type") == "PushEvent")

    def process_text_data(text_data):
        cleaned_text = [re.sub(r'\W+', ' ', text.lower()) for text in text_data]
        return ' '.join(cleaned_text)

    process_text_data_udf = spark.udf.register("process_text_data", process_text_data)
    processed_data = push_events.withColumn("processed_text", process_text_data_udf(col("payload.commits.message")))
    
    tokenizer = Tokenizer(inputCol="processed_text", outputCol="tokenized_words")
    ngram = NGram(n=3, inputCol=tokenizer.getOutputCol(), outputCol="ngrams_result")
    pipeline = Pipeline(stages=[tokenizer, ngram])
    model = pipeline.fit(processed_data)
    result = model.transform(processed_data)

    result = result.withColumn(
        "first_five_words",
        expr("slice(ngrams_result, 1, case when size(ngrams_result) >= 5 then 5 else size(ngrams_result) end)")
    )

    result = result.withColumn("first_five_words", concat_ws(", ", col("first_five_words")))
    result.select("actor.display_login", "first_five_words").coalesce(1).write.csv("output", header=True, mode="overwrite")
    spark.stop()

if __name__ == "__main__":
    main()