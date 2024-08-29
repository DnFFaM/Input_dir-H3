from pyspark import SparkContext, SparkConf
import subprocess
import os

def extract(input_dir):
    conf = SparkConf().setAppName("WordCountApp").setMaster("local")
    sc = SparkContext(conf=conf)
    data = sc.textFile(input_dir)
    return sc, data

def transform(data):
    # Collect all data to the driver node
    word_counts = data.collect()
    
    word_count = {}
    # Process each line
    for line in word_counts:
        words = line.split()
        for word in words:
            cleaned_word = word.strip(', . ; : ? ! () [] {}').lower()
            if cleaned_word:
                word_count[cleaned_word] = word_count.get(cleaned_word, 0) + 1
    return word_count

def load(word_count, output_dir):
    try:
        # Remove the existing output file if it exists
        subprocess.call(["hdfs", "dfs", "-rm", "-r", output_dir], shell=True)
    except Exception as e:
        print(f"\nError deleting old output file: {e}")

    # Write the transformed data to a local file
    local_output_file = "output.txt"
    with open(local_output_file, "w") as f:
        number_of_words = 1
        for word, count in word_count.items():
            f.write(f"\n {number_of_words} {word}: {count}")
            number_of_words += 1
    
    # Upload the local file to HDFS
    subprocess.call(["hadoop", "fs", "-put", local_output_file, output_dir], shell=True)
    
    
    print("Completed")

def main():
    input_dir = "hdfs://localhost:9000/input_dir/AChristmasCarol_CharlesDickens_English.txt"
    output_dir = "hdfs://localhost:9000/input_dir/output.txt"
    
    # ETL process
    sc, data = extract(input_dir)
    word_count = transform(data)
    load(word_count, output_dir)
    
    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()
