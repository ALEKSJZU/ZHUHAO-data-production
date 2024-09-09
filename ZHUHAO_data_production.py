from openai import OpenAI
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
# Replace with your actual API key
api_key = "sk-19357757a8ec42099296a6f2750b967c"
base_url = "https://api.deepseek.com"

client = OpenAI(api_key=api_key, base_url=base_url)

def tag_sentence(word_to_keep, sentence):
    """Tag all words in the sentence except the word_to_keep."""
    tagged_sentence = ""
    words = sentence.split()
    tag_id = 1
    for word in words:
        if word.lower() != word_to_keep.lower():  # Tag words not equal to word_to_keep
            tagged_sentence += f"{{{word}:{tag_id}}} "
            tag_id += 1
        else:
            tagged_sentence += f"{word} "  # Keep the word untagged
    # print(word_to_keep, tagged_sentence)
    return tagged_sentence.strip()

def send_request(tagged_sentence):
    """Send the request to the DeepSeek API for selective translation."""
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "System prompt: Some of the words in the following sentence is tagged with number. Translate only these tagged words in the context of this sentence into Chinese and keep the tags, while keeping other words in English. Example input: The {quick:1} {brown:2} fox jumps over the {lazy:3} {dog:4}. Example output: The {敏捷的:1} {棕色:2} fox jumps over the {懒惰的:3} {狗:4}. As you can see, the tagged words (quick, brown, lazy, dog) are translated into Chinese, while other words are still in English in the output."},
            {"role": "user", "content": tagged_sentence},
        ],
        stream=False,
        temperature=1.3
    )
    return response.choices[0].message.content

def process_row(row, idx):
    """Process a single row from the TSV file."""
    word_to_keep = row[1]  # First column contains the word to keep untagged
    processed_row = [word_to_keep]  # Start the new row with the word in the first column
    
    # Process each sentence column
    results = []
    for i in range(2, len(row)):  # Process all sentence columns
        sentence = row[i]
        tagged_sentence = tag_sentence(word_to_keep, sentence)  # Tag the sentence
        try:
            translated_sentence = send_request(tagged_sentence)  # Send to API and get the result
        except Exception as e:
            print(f"Error processing row {idx}, column {i + 1}: {e}")
            translated_sentence = "ERROR"  # In case of failure, mark the result as ERROR
        
        # Add original and translated sentences side by side
        results.append((tagged_sentence, translated_sentence))
    
    return idx, processed_row, results

def process_tsv(input_tsv, output_tsv):
    with open(input_tsv, mode='r', encoding='utf-8') as infile, open(output_tsv, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile, delimiter='\t')  # Read as TSV
        writer = csv.writer(outfile, delimiter='\t')  # Write as TSV

        futures = []
        results_dict = {}
        with ThreadPoolExecutor(max_workers=200) as executor:
            # Submit tasks for the first 1000 rows
            for idx, row in enumerate(reader):
                if len(row) < 3:
                    continue  # Skip rows without at least 3 columns
                futures.append(executor.submit(process_row, row, idx))

            # Gather results and store them in a dictionary to maintain order
            for future in as_completed(futures):
                idx, processed_row, results = future.result()
                # Store the results in the dictionary with the index as the key
                results_dict[idx] = (processed_row, results)

        # Write the results back in the original order
        for idx in sorted(results_dict.keys()):
            processed_row, results = results_dict[idx]
            for tagged_sentence, translated_sentence in results:
                processed_row.append(tagged_sentence)
                processed_row.append(translated_sentence)
            writer.writerow(processed_row)
            print(f'{idx} row processed.')

# Example usage
input_tsv = 'updated_combined_output.tsv'  # Path to your input CSV file
output_tsv = 'translated_test.tsv'  # Path to your output CSV file
process_tsv(input_tsv, output_tsv)
