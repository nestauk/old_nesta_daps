'''
Clean reviews
=============
Data cleaning and processing procedures for the employee reviews dataset.
Reviews are made lower-case, most punctuation is removed. 
Words and sentences are tokenized
'''

import re
import nltk    
import pandas as pd



# Turn dictionary of reviews into a dataframe
def json_to_dataframe(input_file):

    df_reviews = pd.read_json(input_file, orient='index', convert_axes=False)

    return df_reviews
    

# Clean review text
def clean_review_text(one_review):
    
    # Change to lower case
    one_review = one_review.lower()
    
    # Remove all punctuation except ', -, . and spaces
    one_review = re.sub("[^a-zA-Z'\-. ]+", '', one_review)
    
    # Return
    return one_review


# Tokenize words in reviews
def word_tokenize_review_text(one_review):
    
    # Remove full stops
    one_review = one_review.replace(".","") 

    one_word_tokenized_review = nltk.word_tokenize(one_review)
    
    # Return
    return one_word_tokenized_review

    
# Tokenize sentences in reviews
def sent_tokenize_review_text(one_review):
    
    # Tokensize sentence
    one_sent_tokenized_review = nltk.sent_tokenize(one_review)
    
    # Remove full stops
    one_sent_tokenized_review = [one_sent.replace(".","") for one_sent in one_sent_tokenized_review]
    
    # Return
    return one_sent_tokenized_review


# Run file
if __name__ == '__main__':
    
    # Turn dictionary of reviews into a dataframe
    df_reviews = json_to_dataframe(input_file)
    
    # Clean review text
    df_reviews['review_text'] = df_reviews['review_text'].apply(clean_review_text) 
    
    # Create a new field with tokenized words
    df_reviews['word_tokenized_review_text'] = df_reviews['review_text'].apply(word_tokenize_review_text)
    
    # Create a new field with tokenized sentences
    df_reviews['sent_tokenized_review_text'] = df_reviews['review_text'].apply(sent_tokenize_review_text) 

