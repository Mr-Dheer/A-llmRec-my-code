import numpy as np
from datetime import datetime

def get_answers_predictions(file_path):
    answers = []
    llm_predictions = []
    with open(file_path, 'r') as f:
        # currentTime = datetime.now().strftime("%Y-%m-%d %H:%M")
        # answers.append("Generated on " + currentTime)
        for line in f:
            if line.startswith('Answer:'):
                # Remove the 'Answer:' prefix, strip whitespace and remove the surrounding quotes, then lowercase.
                answer = line.replace('Answer:', '').strip()[1:-1].lower()
                answers.append(answer)
            if line.startswith('LLM:'):
                llm_prediction = line.replace('LLM:', '').strip().lower()
                try:
                    llm_prediction = llm_prediction.replace("\"item title\" : ", '')
                    start = llm_prediction.find('"')
                    end = llm_prediction.rfind('"')
                    # Ensure that the indices are valid
                    if start < 0 or end <= start:
                        raise ValueError("Unexpected format")
                    llm_prediction = llm_prediction[start+1:end]
                except Exception as e:
                    # In case of formatting error, simply keep the (possibly unprocessed) string.
                    pass
                llm_predictions.append(llm_prediction)

    return answers, llm_predictions

def evaluate(answers, llm_predictions, k=1):
    NDCG = 0.0
    HT = 0.0
    predict_num = len(answers)

    for answer, prediction in zip(answers, llm_predictions):
        # Skip counting as correct if both answer and prediction are "no title"
        # if answer == "no title" and prediction == "no title":
        #     continue

        if k > 1:
            try:
                rank = prediction.index(answer)
                if rank < k:
                    NDCG += 1 / np.log2(rank + 1)
                    HT += 1
            except Exception as e:
                # answer not found in prediction
                pass
        elif k == 1:
            # Only count as hit if answer is contained in prediction and it's not the invalid "no title" case.
            if answer in prediction:
                NDCG += 1
                HT += 1

    return NDCG / predict_num, HT / predict_num

if __name__ == "__main__":
    # Changed the location here for the .txt file, also change in the a_llmrec_model.py
    inferenced_file_path = '/home/kavach/Dev/Publication/A-LLM-Rec/A-LLMRec_copy_original/rec_output/Lux-Fixed/google.txt'
    answers, llm_predictions = get_answers_predictions(inferenced_file_path)
    print(len(answers), len(llm_predictions))
    assert (len(answers) == len(llm_predictions))

    ndcg, ht = evaluate(answers, llm_predictions, k=1)
    print(f"ndcg at 1: {ndcg}")
    print(f"hit at 1: {ht}")