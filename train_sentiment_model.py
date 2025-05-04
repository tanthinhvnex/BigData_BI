import findspark
findspark.init()
from pyspark.ml.feature import StringIndexerModel
import time
import sys
import os
import traceback
import re # Vẫn import re để tham khảo logic, nhưng không dùng trực tiếp trong pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.ml.feature import (
    Tokenizer, StringIndexer, CountVectorizer,
    NGram, VectorAssembler, ChiSqSelector, IDF,
    SQLTransformer # <<< THÊM IMPORT NÀY
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from datetime import datetime, timezone
import matplotlib.pyplot as plt

# Attempt to import sklearn, handle if not found
try:
    from sklearn.metrics import ConfusionMatrixDisplay
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("WARNING: scikit-learn not found. Confusion matrix plotting will be skipped.")
    print("Install using: pip install scikit-learn")

# --- Function Definitions ---

def build_ngrams_pipeline(input_col="tweet", target_col="target", n=3): # Đổi tên hàm cho rõ ràng hơn
    """
    Builds a Spark ML Pipeline including text cleaning and using features
    from N-gram sizes 1 up to 'n'.

    Args:
        input_col (str): Name of the column containing the input text (tweet).
        target_col (str): Name of the column containing the target label.
        n (int): The maximum N-gram size to use (e.g., 3 for unigrams, bigrams, trigrams).

    Returns:
        pyspark.ml.Pipeline: The configured ML Pipeline object.
    """
    # ---- Giữ lại STAGE TIỀN XỬ LÝ BẰNG SQLTRANSFORMER ----
    cleaned_text_col = "cleaned_text_for_tokenizer"
    sql_statement = f"""
        SELECT *,
               regexp_replace(
                   regexp_replace(
                       lower(trim({input_col})),
                       'http\\\\S+|www\\\\.\\\\S+', ''
                   ),
                   '[^a-zA-Z\\\\n ]', ''
               ) AS {cleaned_text_col}
        FROM __THIS__
    """
    print(f"Adding SQLTransformer stage with statement:\n{sql_statement}")
    cleaner = SQLTransformer(statement=sql_statement) # Stage đơn lẻ
    # ---- KẾT THÚC SQLTRANSFORMER ----

    print(f"Building pipeline using features for n=1 to {n}, input='{input_col}' (cleaned), target='{target_col}'")

    # ---- Giữ lại TOKENIZER DÙNG CỘT ĐÃ LÀM SẠCH ----
    tokenizer = Tokenizer(inputCol=cleaned_text_col, outputCol="words") # Stage đơn lẻ
    # ---- KẾT THÚC TOKENIZER ----

    # --- TẠO LIST STAGES CHO CÁC CỠ N-GRAM, CV, IDF ---
    ngram_stages = [ # List nhiều stages
        NGram(n=i, inputCol="words", outputCol=f"{i}_grams")
        for i in range(1, n + 1)
    ]
    cv_stages = [ # List nhiều stages
        CountVectorizer(vocabSize=2**14, inputCol=f"{i}_grams", outputCol=f"{i}_tf")
        for i in range(1, n + 1)
    ]
    idf_stages = [ # List nhiều stages
        IDF(inputCol=f"{i}_tf", outputCol=f"{i}_tfidf", minDocFreq=5)
        for i in range(1, n + 1)
    ]
    # ---------------------------------------------------------------

    assembler = VectorAssembler( # Stage đơn lẻ
        inputCols=[f"{i}_tfidf" for i in range(1, n + 1)],
        outputCol="rawFeatures"
    )
    # ---------------------------------------------------------------

    # --- Tạo các stage đơn lẻ còn lại ---
    label_stringIdx = StringIndexer(inputCol=target_col, outputCol="label", stringOrderType="alphabetAsc") # Stage đơn lẻ
    selector = ChiSqSelector(numTopFeatures=2**14, featuresCol='rawFeatures', outputCol="features", labelCol="label") # Stage đơn lẻ
    lr = LogisticRegression(featuresCol="features", labelCol="label", regParam=0.1, maxIter=1000, elasticNetParam=0.0) # Stage đơn lẻ
    # ----------------------------------------------------

    # ---- XÂY DỰNG LIST STAGES CUỐI CÙNG ----
    # Nối các stage đơn lẻ và các list stages lại
    all_stages_list = [cleaner, tokenizer] + ngram_stages + cv_stages + idf_stages + [assembler, label_stringIdx, selector, lr]
    pipeline = Pipeline(stages=all_stages_list)
    # ---- KẾT THÚC XÂY DỰNG LIST ----
    return pipeline



def main():
    """
    Main function to run the Spark ML Twitter Sentiment Analysis pipeline.
    """
    print("--- Script Start ---")
    spark1 = None
    pipelineFit = None

    try:
        # --- Spark Session Creation & Initialization ---
        print("Creating Spark Session...")
        spark1 = SparkSession.builder\
                    .master("local[*]")\
                    .appName("LR_Twitter_Sentiment_Train_With_Cleaning_Multi_Ngram")\
                    .config("spark.driver.memory", "6g")\
                    .getOrCreate()
        print(f"Spark Session created: {spark1}")
        print(f"Spark Version: {spark1.version}")
        spark1.sql("SELECT 1").show()
        print("Spark Session seems responsive initially.")

        # --- Data Loading and Schema Definition ---
        path = "D:/Downloads/training_sentiment.csv"
        print(f"Dataset path: {path}")
        schema = StructType([
            StructField("target", IntegerType(), True),
            StructField("id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("query", StringType(), True),
            StructField("author", StringType(), True),
            StructField("tweet", StringType(), True) # Input gốc vẫn là 'tweet'
        ])
        print("Schema defined.")

        print("Loading data from CSV...")
        df = spark1.read.csv(path, header=False, schema=schema, encoding='latin1')
        print("Data loaded successfully.")

        # --- Data Splitting ---
        print("Splitting data into training and testing sets (80/20 split)...")
        (train_set, test_set) = df.randomSplit([0.80, 0.20], seed = 2000)

        # --- <<< QUAN TRỌNG: ÁP DỤNG SAMPLE *SAU KHI* SPLIT >>> ---
        # train_set = train_set.sample(fraction=0.00001, withReplacement=False, seed = 43)
        # test_set = test_set.sample(fraction=0.0001, withReplacement=False, seed = 44)
        # --- <<< KẾT THÚC SAMPLE >>> ---

        print(f"Training set count after sampling: {train_set.count()}")
        print(f"Test set count after sampling: {test_set.count()}")
        train_set.cache()
        test_set.cache()

                # --- Pipeline Building ---
        print("Building the ML pipeline instance (now includes cleaning and multi n-grams)...")
        # Gọi hàm build_ngrams_pipeline đã được sửa đổi
        pipeline = build_ngrams_pipeline(input_col="tweet", target_col="target", n=3) # Sử dụng hàm mới
        print("Pipeline definition:")
        # ---- SỬA LẠI VÒNG LẶP IN ----
        for i, stage in enumerate(pipeline.getStages()):
            # Lấy thông tin input
            input_info = "N/A"
            if hasattr(stage, 'getInputCol') and stage.isSet(stage.inputCol): # Ưu tiên kiểm tra inputCol đã set chưa
                input_info = stage.getInputCol()
            elif hasattr(stage, 'getInputCols') and stage.isSet(stage.inputCols):
                input_info = stage.getInputCols()
            elif hasattr(stage, 'getInputCol'): # Fallback nếu isSet không hoạt động (ít xảy ra)
                 try: input_info = stage.getInputCol()
                 except: pass
            elif hasattr(stage, 'getInputCols'): # Fallback nếu isSet không hoạt động
                 try: input_info = stage.getInputCols()
                 except: pass

            # Lấy thông tin output
            output_info = "N/A"
            if hasattr(stage, 'getOutputCol') and stage.isSet(stage.outputCol): # Ưu tiên kiểm tra outputCol đã set chưa
                 output_info = stage.getOutputCol()
            elif hasattr(stage, 'getOutputCols') and stage.isSet(stage.outputCols):
                 output_info = stage.getOutputCols()
            elif hasattr(stage, 'getOutputCol'): # Fallback
                 try: output_info = stage.getOutputCol()
                 except: pass
            elif hasattr(stage, 'getOutputCols'): # Fallback
                 try: output_info = stage.getOutputCols()
                 except: pass

            print(f"  Stage {i}: {stage.__class__.__name__} (Input(s): {input_info}, Output: {output_info})")
        # ---- KẾT THÚC SỬA VÒNG LẶP IN ----

        # --- Model Training ---
        print("Starting model training...")
        start_time = time.time()
        st_datetime = datetime.now(timezone.utc)

        pipelineFit = pipeline.fit(train_set) # Fit pipeline bao gồm cả cleaner

        # Tìm StringIndexerModel trong pipeline đã fit
        string_indexer_model = None
        for stage in pipelineFit.stages:
            # Kiểm tra xem stage có phải là StringIndexerModel không VÀ output của nó là 'label'
            if isinstance(stage, StringIndexerModel) and stage.getOutputCol() == "label":
                string_indexer_model = stage
                break

        if string_indexer_model:
            print("\n--- StringIndexer Label Mapping ---")
            # labels là một list các nhãn gốc, thứ tự của chúng tương ứng với index 0.0, 1.0, ...
            original_labels = string_indexer_model.labels
            print(f"Original labels ordered by index: {original_labels}")
            for index, label in enumerate(original_labels):
                sentiment = "Positive" if label == '4' else "Negative" if label == '0' else "Unknown"
                print(f"Index {float(index)} corresponds to original label '{label}' ({sentiment})")
            print("-----------------------------------\n")
        else:
            print("Could not find the fitted StringIndexerModel for 'label' output.")

        training_time = time.time() - start_time
        print(f"Model training completed in: {training_time:.2f} seconds")
        print(f"(Timestamp method) Training time: {datetime.now(timezone.utc) - st_datetime}")

        # --- Prediction ---
        print("Making predictions on the test set...")
        # Transform sẽ tự động chạy cleaner stage trước
        predictions = pipelineFit.transform(test_set)
        print("Predictions generated.")

        # --- Evaluation ---
        print("Evaluating model performance...")
        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label") # labelCol vẫn là output của StringIndexer
        accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
        precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
        recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
        f1_score = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

        print("\n--- Evaluation Metrics ---")
        print(f"Accuracy:  {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall:    {recall:.4f}")
        print(f"F1 Score:  {f1_score:.4f}")
        print("--------------------------\n")

        # --- <<< LƯU MODEL >>> ---
        if pipelineFit is not None:
            model_save_path = f"pre_trained_model"
            print(f"Attempting to save the trained pipeline model (with cleaning stage and multi n-grams) to: {model_save_path}")
            try:
                pipelineFit.write().overwrite().save(model_save_path)
                print(f"Trained pipeline model successfully saved to: {model_save_path}")
                # Kiểm tra xem stage SQLTransformer có được lưu không (thường là có)
                print(f"Contents of {os.path.abspath(model_save_path)}:")
                for item in os.listdir(os.path.abspath(model_save_path)):
                    print(f"- {item}")
            except Exception as save_error:
                print(f"ERROR saving the model: {save_error}", file=sys.stderr)
                traceback.print_exc()
        else:
             print("WARNING: pipelineFit object is None, cannot save the model.", file=sys.stderr)
        # --- <<< KẾT THÚC LƯU MODEL >>> ---

        # --- Confusion Matrix Plotting ---
        if SKLEARN_AVAILABLE:
            print("Generating Confusion Matrix...")
            try:
                print("Collecting labels and predictions to driver memory...")
                y_true_collected = predictions.select('label').collect()
                y_pred_collected = predictions.select('prediction').collect()
                print(f"Collected {len(y_true_collected)} labels/predictions.")

                y_true = [row['label'] for row in y_true_collected]
                y_pred = [row['prediction'] for row in y_pred_collected]

                # Tìm StringIndexerModel trong pipeline mới
                si_model_stage = None
                if pipelineFit is not None and hasattr(pipelineFit, 'stages'):
                    # Thường thì StringIndexer sẽ nằm gần cuối, trước các stage thuật toán
                    for stage in reversed(pipelineFit.stages):
                        if isinstance(stage, StringIndexerModel) and stage.getOutputCol() == 'label':
                            si_model_stage = stage
                            break

                if si_model_stage:
                    # Lấy nhãn gốc theo đúng thứ tự index
                    original_labels_ordered = si_model_stage.labels
                    # Tạo mapping từ label số (0.0, 1.0) sang nhãn gốc
                    label_map = {float(i): label for i, label in enumerate(original_labels_ordered)}
                    # Xác định nhãn số và nhãn hiển thị
                    class_labels_numeric = sorted(label_map.keys()) # [0.0, 1.0]
                    # Tạo nhãn hiển thị dựa trên mapping đã biết (0 -> Negative, 4 -> Positive)
                    # Giả sử mapping là 0->Neg, 4->Pos
                    if label_map[0.0] == '0' and label_map[1.0] == '4':
                         class_labels_display = ["Negative (0)", "Positive (4)"]
                    elif label_map[0.0] == '4' and label_map[1.0] == '0':
                         class_labels_display = ["Positive (4)", "Negative (0)"]
                    else: # Trường hợp khác, chỉ hiển thị giá trị gốc
                         class_labels_display = [f"{label_map[lbl]} ({lbl:.1f})" for lbl in class_labels_numeric]

                else:
                    print("WARNING: Could not find StringIndexerModel stage. Using default labels.", file=sys.stderr)
                    class_labels_numeric = sorted(list(set(y_true)))
                    class_labels_display = [str(lbl) for lbl in class_labels_numeric]

                print("Plotting confusion matrix...")
                cm_display = ConfusionMatrixDisplay.from_predictions(
                    y_true,
                    y_pred,
                    labels=class_labels_numeric,
                    display_labels=class_labels_display,
                    cmap=plt.cm.Blues
                )
                plt.title("Confusion Matrix")
                plt.show()
                print("Confusion matrix displayed.")

            except Exception as plot_error:
                print(f"ERROR generating confusion matrix: {plot_error}", file=sys.stderr)
                traceback.print_exc()
        else:
            print("Skipping confusion matrix plotting as scikit-learn is not available.")


    except Exception as e:
        print(f"\n--- An ERROR occurred during execution ---", file=sys.stderr)
        print(f"Error details: {e}", file=sys.stderr)
        traceback.print_exc()
        print("--------------------------------------------", file=sys.stderr)

    finally:
        if spark1 is not None:
            print("\nStopping Spark Session...")
            spark1.stop()
            print("Spark Session stopped.")
        print("--- Script End ---")


# --- Script Entry Point ---
if __name__ == "__main__":
    main()