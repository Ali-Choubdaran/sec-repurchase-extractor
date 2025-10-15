#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 13:21:39 2025

@author: a.choubdaran-varnosfaderani
"""


from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re

from sec_api import QueryApi
from sec_api import ExtractorApi
from sec_api import RenderApi
from sec_api import XbrlApi


from collections import Counter

import calendar


import os

import copy 





import os
from typing import Optional
from sec_api import QueryApi, ExtractorApi  # assuming you're using sec-api.com


def add_row_to_dataframe(data_dict, dataframe):
    # Convert the dictionary to a DataFrame
    new_row = pd.DataFrame([data_dict])  # The argument is a list of dictionaries
    # Append the new row to the DataFrame and return
    return pd.concat([dataframe, new_row], ignore_index=True)


def unit_extractor(text):
    if pd.isna(text):
        return text  # Return None if text is NaN

    # Search for units in the text
    units_regex = r"\b(thousands?|millions?|billions?)\b"
    
    # Check if the unit is within parentheses
    parenthetical_regex = r"\(([^)]*?" + units_regex + r"[^)]*?)\)"
    
    # First, try finding the unit inside parentheses
    paren_match = re.search(parenthetical_regex, text, re.IGNORECASE)
    if paren_match:
        return paren_match.group(1)  # Return just the content inside the parentheses
    
    # If not inside parentheses, check for the presence of unit anywhere in the text
    unit_match = re.search(units_regex, text, re.IGNORECASE)
    if unit_match:
        return text  # Return the whole text if the unit isn't specifically within parentheses

    return np.nan  # Return None if no unit is found


def unit_analyser(text):
    # Return the text if it is NaN
    if pd.isna(text):
        return text
    
    # Initialize the dummy variable
    except_exist = 0

    # Replace every non-alphabetic character with a whitespace
    cleaned_text = re.sub(r'[^a-zA-Z]', ' ', text)
    
    # Split the cleaned text into words
    words = cleaned_text.split()
   
    if len(words)>20:
        return f'y{len(words)}'
    
    # Mapping of plural words to their singular forms
    singular_map = {
        "thousands": "thousand",
        "millions": "million",
        "billions": "billion",
        "dollars": "dollar",
        "shares": "share",
        "amounts": "amount",
        "numbers": "number",
        "values": "value",
        "figures": "figure",
        
    }
    
    units = {'thousand', 'million', 'billion'}
    
    # Replace plural words with their singular forms if they are in the map
    singular_words = [singular_map[word.lower()] if word.lower() in singular_map else word for word in words]

    # Create singular_words2, excluding words of length one
    singular_words2 = [word for word in singular_words if len(word) > 2]

    # Create singular_words3, excluding the words 'in' and 'of'
    exclude_words = {'in', 'of'}  # Set of words to exclude
    singular_words3 = [word for word in singular_words2 if word.lower() not in exclude_words]

    # Create singular_words4 by merging "per share" into a single element
    singular_words4 = []
    i = 0
    while i < len(singular_words3):
        if i < len(singular_words3) - 1 and singular_words3[i].lower() == 'per' and singular_words3[i + 1].lower() == 'share':
            singular_words4.append('per share')
            i += 2  # Skip the next word as it has been merged
        else:
            singular_words4.append(singular_words3[i])
            i += 1
            
    exclude_words ={"number","value","amount","figure","are","and","stated","expressed","presented","denoted",'price','paid','reflected','which','total'} 
    
    singular_words4 = [word for word in singular_words4 if word.lower() not in exclude_words]
    
    
    keep_words={'dollar','share','per share','except','million','thousand','billion'}
    
    
    exotic_words=[word for word in singular_words4 if word.lower() not in keep_words]
    
    if len(exotic_words)>2:
        return 'y'
    
    
    
    # Otherwise, perform the original counting and summing of units
    unit_count_dict = Counter(word for word in singular_words4 if word.lower() in units)
    tot_unit_count = sum(unit_count_dict.values())
    # Check for the presence of "except" and set except_exist
    if 'except' in singular_words4:
        except_exist = 1

    # Process when there is exactly one element left and it's a unit
    if len(singular_words4) == 1:
        unit_scale = {'thousand': 1, 'million': 2, 'billion': 3}
        return unit_scale.get(singular_words4[0].lower(), 'y')

    # Handle case when there are exactly two elements
    if len(singular_words4) == 2:
        # Define unit types and their corresponding codes
        unit_codes = {'thousand': '1', 'million': '2', 'billion': '3'}
        other_words = {'share', 'dollar'}
        other_word_codes = {'share': 's', 'dollar': 'd'}

        # Determine which word is the unit and which is 'share' or 'dollar' or 'per share'
        unit_word = next((word for word in singular_words4 if word.lower() in units), None)
        other_word = next((word for word in singular_words4 if word.lower() in other_words), None)

        if unit_word and other_word:
            # Return combined code based on the unit and the other word
            return other_word_codes.get(other_word.lower(),'y') + unit_codes.get(unit_word.lower(),0)
        elif unit_word:
            # Return 'y' prefixed code if other word is not 'share' or 'dollar' or 'per share'
            return 'y' + unit_codes.get(unit_word.lower(),0)
    
    if len(singular_words4) > 2 and  except_exist==1 and tot_unit_count == 1:
        except_index = singular_words4.index('except')
        unit_word = next((word for word in singular_words4 if word.lower() in units), None)
        unit_index = singular_words4.index(unit_word)
        unit_location = 1 if unit_index < except_index else 0
        # Determine keywords before and after 'except'
        key_words = {'dollar', 'share', 'per share'}
        key_words_before_except = set()
        key_words_after_except = set()
    
        for i, word in enumerate(singular_words4):
            if word.lower() in key_words:
                if i < except_index:
                    key_words_before_except.add(word)
                else:
                    key_words_after_except.add(word)
        if len(key_words_before_except)==0 and unit_location==1:
            if {'per share'}==key_words_after_except:
                unit_scale = {'thousand': 'a1', 'million': 'a2', 'billion': 'a3'}
                return unit_scale.get(unit_word.lower(), 'y0')
            if {'per share', 'share'}==key_words_after_except:
                unit_scale = {'thousand': 'd1', 'million': 'd2', 'billion': 'd3'}
                return unit_scale.get(unit_word.lower(), 'y')
            
        if key_words_before_except=={"dollar"} and unit_location==1:
            if {'per share'}==key_words_after_except:
                unit_scale = {'thousand': 'd1', 'million': 'd2', 'billion': 'd3'}
                return unit_scale.get(unit_word.lower(), 'y')
            
        if key_words_before_except=={"share"} and unit_location==1:
            if {'per share'}==key_words_after_except:
                unit_scale = {'thousand': 's1', 'million': 's2', 'billion': 's3'}
                return unit_scale.get(unit_word.lower(), 'y')
            
    if len(singular_words4) > 2  and tot_unit_count == 2:

        filtered_list = [word for word in singular_words4 if word.lower() in units or word.lower() in {'share', 'dollar'}]
        # If the remaining list has 4 elements and two are units
        if len(filtered_list) == 4:
            unit1 = next((word for word in filtered_list if word.lower() in units), None)
            filtered_list.remove(unit1)
            unit2 = next((word for word in filtered_list if word.lower() in units), None)
            filtered_list.remove(unit2)
            
            other1 = filtered_list[0]
            other2 = filtered_list[1]
    
            unit_codes = {'thousand': '1', 'million': '2', 'billion': '3'}
            other_codes = {'share': 's', 'dollar': 'd'}
            
            return other_codes.get(other1.lower(),'y') + unit_codes[unit1.lower()] + other_codes.get(other2.lower(),'y') + unit_codes[unit2.lower()]
        
        # If the remaining list has 4 elements and two are units
        if len(filtered_list) == 3:
            unit1 = next((word for word in filtered_list if word.lower() in units), None)
            filtered_list.remove(unit1)
            unit2 = next((word for word in filtered_list if word.lower() in units), None)
            filtered_list.remove(unit2)
            
            other = filtered_list[0]
           
    
            unit_codes = {'thousand': '1', 'million': '2', 'billion': '3'}
            other_codes = {'share': 's', 'dollar': 'd'}
            
            remaining_other_code = next(value for key, value in other_codes.items() if key != other.lower())
                
            return remaining_other_code + unit_codes[unit1.lower()] + other_codes.get(other.lower(),'y') + unit_codes[unit2.lower()]
    
                    
        

    return 'y'


# Function to add spaces between text nodes in the HTML
def preprocess_html(t):
    
    
    # Iterate over all text nodes in the HTML
    for element in t.find_all(text=True):
        # Add a space after each text node
        element.replace_with(f"{element} ")
    
    return t



def convert_to_string_if_not_nan(t):
    # Check if the input is NaN
    if pd.isna(t):
        return t  # Return as is if it's NaN
    
    # Convert to string if not NaN and not already a string
    if not isinstance(t, str):
        return str(t)
    return t

def unicode_text_cleaner(t):
    if pd.isna(t):
        return t
    # Replace specific characters with whitespace
    t = t.replace('\u00a0', ' ').replace('\u200b', ' ').replace('\u200c', ' ')

    # Replace consecutive whitespaces with a single space
    t = re.sub(r"\s+", " ", t)
    t=t.strip()
    return (t)


def convert_and_parenthesize_superscripts(text):
    if pd.isna(text):
        return text
    # Map of superscript numerals to their normal digit counterparts
    superscript_map = str.maketrans('¹²³⁴⁵⁶⁷⁸⁹', '123456789')
    
    # Function to convert superscripts in a match to parenthesized digits
    def replace_with_parentheses(match):
        # Translate superscript to regular digits and wrap each digit with parentheses
        return ''.join(f'({digit})' for digit in match.group().translate(superscript_map))
    
    # Regex to find sequences of superscript characters
    converted_text = re.sub(r'[¹²³⁴⁵⁶⁷⁸⁹]+', replace_with_parentheses, text)
    
    return converted_text


                
def check_issuer_in_text(text):
    # Return the text if it is NaN
    if pd.isna(text):
        return text
    
    # Search for 'Issuer' in the text using regular expression for case-insensitivity
    if re.search(r'Issuer', text, re.IGNORECASE):
        # Check if there is a parenthesis in the text
        match = re.search(r'\(([^)]*)\)', text)
        if match:
            # Return the content inside the parenthesis
            return match.group(0)
        else:
            return np.nan  # Return np.nan if no parenthesis is found
    else:
        return text  # Return the original text if 'Issuer' is not found
    
    
    
def para_whitespace_stripper(text):
    if pd.isna(text):
        return text  # Return NaN if the input is NaN
    # Pattern to find and remove spaces after an opening parenthesis
    text = re.sub(r'\(\s+', '(', text)
    
    # Pattern to find and remove spaces before a closing parenthesis
    text = re.sub(r'\s+\)', ')', text)
    
    return text


def check_healthy_parentheses(text):
    if pd.isna(text):
        return text  # Return NaN if the input is NaN
    
    # Count occurrences of each type of parenthesis
    open_parentheses = text.count('(')
    close_parentheses = text.count(')')
    
    # Check the counts and return accordingly
    if open_parentheses == 0 and close_parentheses == 0:
        return 0  # No parentheses at all
    elif open_parentheses == close_parentheses:
        return 1  # Healthy parentheses (matched counts)
    else:
        return -1  # Unhealthy parentheses (unmatched counts)




#let's take care of edge cases regardint experssing units:
# version 1
def three_zero_to_thousand(text):
    if pd.isna(text):
        return text

    # Regular expression to match 000s, 000's, 000,s within parentheses
    match_paren = re.search(r'\(([^)]*000[^)]*)\)', text)
    
    if match_paren:
        # Extract the text inside the parentheses
        inside_text = match_paren.group(1)
        
        # Remove non-alphabetic and non-digit characters and check if it is 000s or 000
        cleaned_text = re.sub(r'[^a-zA-Z0-9]', '', inside_text)
        if cleaned_text in ['000s', '000']:
            return re.sub(r'\([^)]*\)', '(thousand)', text)

    # Regular expression to match "in 000s", "in 000's", "in 000,s"
    match_in = re.search(r'\bin\s*000[\'s,]*\b', text)
    
    if match_in:
        # Replace "in 000s", "in 000's", "in 000,s" with "in thousand"
        return re.sub(r'\bin\s*000[\'s,]*\b', 'in thousand', text)

    return text



# version 2
def three_zero_to_thousand(text):
    if pd.isna(text):
        return text

    # Regular expression to match 000s, 000's, 000,s within parentheses
    def replace_with_thousand(match):
        # Extract the text inside the parentheses
        inside_text = match.group(1)
        
        # Remove non-alphabetic and non-digit characters and check if it is 000s or 000
        cleaned_text = re.sub(r'[^a-zA-Z0-9]', '', inside_text)
        if cleaned_text in ['000s', '000']:
            return '(thousand)'
        else:
            return f'({inside_text})'  # return original text if it does not match

    text = re.sub(r'\(([^)]*000[^)]*)\)', replace_with_thousand, text)

    # Regular expression to match "in 000s", "in 000's", "in 000,s"
    text = re.sub(r'\bin\s*000[\'s,]*\b', 'in thousand', text)

    return text




def dollar_sign_to_dollar_word(text):
    if pd.isna(text):
        return text

    # Regular expression to match $ or $s followed immediately by "in" (ignoring whitespace)
    match = re.search(r'\$\s*in|\$\s*s\s*in', text, re.IGNORECASE)
    
    if match:
        # Replace $ or $s followed by "in" with "dollar"
        text = re.sub(r'\$\s*(s\s*)?in', 'dollar in', text, flags=re.IGNORECASE)
    
    return text



def text_reducer(text):
    if pd.isna(text):
        return text

    # Remove content inside parentheses
    text = re.sub(r'\(.*?\)', '', text)

    # Remove non-alphabet characters
    text = re.sub(r'[^a-zA-Z\s]', '', text)

    # Remove specific substrings (corrected to handle optional plural 's')
    text = re.sub(r'\b(billions?|millions?|thousands?)\b', '', text, flags=re.IGNORECASE)

    # Collapse multiple whitespace characters into a single space
    text = re.sub(r'\s+', ' ', text)

    # Return the processed text, stripped of leading/trailing whitespace
    return text.strip()



def check_single_digit_or_letter(text):
    if pd.isna(text):
        return True
    # Match single digit or single letter within parentheses
    return bool(re.match(r'^\([0-9a-zA-Z]\)$', text))

 
def check_single_digit_or_letter(text):
    # Check if the input is NaN
    if pd.isna(text):
        return True

    # Check if the input is not a string
    if not isinstance(text, str):
        return False

    # Strip whitespace from the text
    stripped_text = text.strip()

    # Regex to match one or more footnote-like patterns within the text, allowing for various separators
    footnote_pattern = r'^(\([0-9a-zA-Z]\)([ ,/])?)+$'

    # Match multiple single-digit or single-letter footnotes within parentheses, potentially separated
    # by spaces, commas, or slashes
    return bool(re.match(footnote_pattern, stripped_text))



def bracket_to_paranth(text):
    if pd.isna(text):
        return text
    if not isinstance(text, str):
        return text
    
    # Replace opening square brackets with opening parentheses
    text = text.replace('[', '(')
    # Replace closing square brackets with closing parentheses
    text = text.replace(']', ')')
    
    return text

                
def check_only_dollar_sign(text):
    if pd.isna(text):
        return True
    # Check if the text is exactly a dollar sign
    return text.strip() == '$'



# Function to drop duplicate columns
def drop_duplicate_columns(df):
    # Transpose the DataFrame to treat columns as rows for easy duplicate removal
    df_transposed = df.T

    # Drop duplicates: keep='first' ensures the first occurrence is not considered a duplicate
    df_transposed = df_transposed.drop_duplicates(keep='first')

    # Transpose back to the original form
    df = df_transposed.T

    return df
                


def text_reducer2(text):
    if pd.isna(text):
        return text

    # Remove content inside parentheses
    text = re.sub(r'\(.*?\)', ' ', text)

    # Remove non-alphabet characters
    text = re.sub(r'[^a-zA-Z\s]', ' ', text)

    # Remove specific substrings (corrected to handle optional plural 's')
    text = re.sub(r'\b(billions?|millions?|thousands?)\b', ' ', text, flags=re.IGNORECASE)

    # Collapse multiple whitespace characters into a single space
    text = re.sub(r'\s+', ' ', text)

    # Return the processed text, stripped of leading/trailing whitespace
    return text.strip()


def white_word_maker(text):
    # If the text is NaN, return an empty list
    if pd.isna(text):
        return []
    
    # Convert text to lowercase
    text = text.lower()
    
    # Break the text into words based on whitespace
    words = text.split()
    
    # Drop any word that has length 2 or less
    filtered_words = [word for word in words if len(word) > 2]
    
    return filtered_words



#footnote handling
def extract_potential_footnotes(soup):
    di = {}
    
    for idx, x in enumerate(soup.contents):
        y = x.get_text(separator=' ', strip=True)
        
        if not y:
            continue
        
        # Find the first alphanumeric character after ignoring non-alphanumeric characters
        match = re.search(r'[^a-zA-Z0-9]*([a-zA-Z0-9])', y)
        
        if match:
            footnote = match.group(1)
            # Check loneliness based on the type of character found
            if footnote.isdigit():
                # Lonely digit if the next character is not a digit
                pos = match.end(1)
                if pos == len(y) or not y[pos].isdigit():
                    di[idx] = footnote
            elif footnote.islower():
                # Lonely small cap letter if the next character is not a small cap letter
                pos = match.end(1)
                if pos == len(y) or not y[pos].islower():
                    di[idx] = footnote
            elif footnote.isupper():
                # Lonely large cap letter if the next character is not an alphabetic character
                pos = match.end(1)
                if pos == len(y) or not y[pos].isalpha():
                    di[idx] = footnote
    
    return di




def check_footnotes_type(cand_footnotes):
    if not cand_footnotes:
        return True  # No footnotes to check

    # Determine the type of the first footnote
    first_footnote = next(iter(cand_footnotes.values()))
    if first_footnote.isdigit():
        footnote_type = 'digit'
    elif first_footnote.islower():
        footnote_type = 'lower'
    elif first_footnote.isupper():
        footnote_type = 'upper'
    else:
        return False  # Should not happen as we only allow digits and letters

    # Check if all footnotes are of the same type
    for footnote in cand_footnotes.values():
        if footnote_type == 'digit' and not footnote.isdigit():
            return False
        elif footnote_type == 'lower' and not footnote.islower():
            return False
        elif footnote_type == 'upper' and not footnote.isupper():
            return False
    
    return True

def check_footnotes_consecutive(cand_footnotes):
    if not cand_footnotes:
        return True  # No footnotes to check

    footnotes = list(cand_footnotes.values())
    
    if footnotes[0].isdigit():
        footnote_values = [int(f) for f in footnotes]
    else:
        footnote_values = [ord(f.lower()) for f in footnotes]

    return footnote_values == list(range(min(footnote_values), max(footnote_values) + 1))



def out_paranth_footnote_into_paranth(text, footnotes):
    if pd.isna(text):
        return text
    if len(footnotes)==0:
        return text

    # Convert text to string if not already
    text_org=text
    change_made=0
    text = str(text).strip()

    # Replace multiple whitespaces with a single '@' symbol
    text = re.sub(r'\s+', '@', text)

    # Split text into words around '@', '(', ')', and ',' and keep these characters as separate elements
    words = []
    parts = re.split(r'([@(),])', text)  # Split and keep '@', '(', ')', and ','
    for part in parts:
        if part not in ['@', '(', ')', ',']:
            # Split further by non-alphabetic and non-digit characters, preserving numbers and letters together
            sub_parts = re.split(r'(?<=[a-zA-Z])(?=[0-9])|(?<=[0-9])(?=[a-zA-Z])|[^a-zA-Z0-9]+', part)
            words.extend(sub for sub in sub_parts if sub)
        else:
            words.append(part)

    # Add '@' to the beginning and end of the list to indicate original whitespace positions
    words = ['@'] + words + ['@']
    
    new_words=[]
    
    for i in range(len(words)):
        if words[i]=="@":
            new_words.append(words[i])
        else:
            if words[i-1]!="@":
                new_words.append("@")
            new_words.append(words[i])
            
    new_words2=[]
    
    for i in range(len(new_words)-1):
        if new_words[i]=="@" and new_words[i+1]=="@":
            continue 
        new_words2.append(new_words[i])
    
    if new_words2[-1]!="@":
        new_words2.append('@')
                
    words=new_words2
    # Convert footnotes to a set for efficient checking
    footnotes_set = set(footnotes.values())

    # Iterate through the list from the start and encapsulate footnotes in parentheses if followed by ',' or '@'
    for i in range(1, len(words) - 1):
        if words[i] not in footnotes_set and words[i] not in [',', '@']:
            break

        if words[i] in footnotes_set:
            change_mdae=1
            words[i] = f"({words[i]})"

    # Iterate through the list from the end and encapsulate footnotes in parentheses if preceded by ',' or '@'
    for i in range(len(words) - 2, 0, -1):
        if words[i] not in footnotes_set and words[i] not in [',', '@']:
            break

        if words[i] in footnotes_set:
            change_made=1
            words[i] = f"({words[i]})"

    # Join words back to form the processed text
    if change_made==1:
        processed_text = ''.join(words).replace('@', ' ').strip()
    if change_made==0:
        processed_text=text_org

    return processed_text


def footnote_remover(text, footnotes):
    if pd.isna(text):
        return text

    # Create a pattern from the footnote values that detects them within parentheses with optional spaces
    footnote_values = set(footnotes.values())  # Extracting values to ensure uniqueness
    pattern = r'\(\s*(' + '|'.join(re.escape(fn) for fn in footnote_values) + r')\s*\)'
    
    # Use regular expression to find and replace footnotes found in the text
    new_text, count = re.subn(pattern, '', text)
    
    # Remove any leading or trailing commas after removing footnotes
    new_text = re.sub(r'^,|,$', '', new_text.strip())
    
    
    
    # Return the original text if no changes were made; otherwise, return the processed text
    return new_text.strip() if count > 0 else text





def table_footnote_extractor(text, footnotes):
    if pd.isna(text):
        return text  # Return None if text is NaN

    # Create a pattern from the footnote values to detect them within parentheses
    footnote_values = set(footnotes.values())  # Ensure unique footnote values
    pattern = r'\(\s*(' + '|'.join(re.escape(fn) for fn in footnote_values) + r')\s*\)'

    # Find all matches for the pattern in the text
    matches = re.findall(pattern, text)

    # If matches are found, return them; otherwise, return None
    return matches if matches else None


                    
def ends_text_strip(text):
    if pd.isna(text):
        return text
    text = re.sub(r"\s+", " ", text)
    text = text.lower()
    return text.strip()



# Define the function to convert a string into its pattern representation
def convert_to_pattern(text):
    if pd.isna(text) or text.strip() == '':
        return []
    # Define regex patterns for different components
    month_pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Sept)'
    digit_pattern = r'\d+'
    
    # Initialize the result pattern list
    result_pattern = []
    
    # Split the text into parts and convert each part to its pattern
    parts = re.split(r'(\W+)', text)  # Split by non-word characters
    for part in parts:
        matched = False
        part = part.strip()  # Strip each part to remove leading/trailing whitespace
        
        # Check for month names
        if re.fullmatch(month_pattern, part, re.IGNORECASE):
            result_pattern.append('month_name')
            matched = True
        # Check for numbers and categorize based on the range
        elif re.fullmatch(digit_pattern, part):
            num = int(part)  # Convert part to integer
            if 1990 <= num <= 2025:
                result_pattern.append('num_label_y')
            elif 1 <= num <= 32:
                result_pattern.append('num_label_dm')
            else:
                result_pattern.append('a number')
            matched = True

        # For any unmatched parts, just add them as is, if they're not empty
        if not matched and part:
            result_pattern.append(part)
    
    return result_pattern



def convert_to_pattern_without_comma(text):
    char_list=[',',':','.','-','_','–']
    if pd.isna(text) or text.strip() == '':
        return []
    # Define regex patterns for different components
    month_pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Sept)'
    digit_pattern = r'\d+'
    
    # Initialize the result pattern list
    result_pattern = []
    
    # Split the text into parts and convert each part to its pattern
    parts = re.split(r'(\W+)', text)  # Split by non-word characters
    for part in parts:
        matched = False
        part = part.strip()  # Strip each part to remove leading/trailing whitespace
        
        # Check for month names
        if re.fullmatch(month_pattern, part, re.IGNORECASE):
            result_pattern.append('month_name')
            matched = True
        # Check for numbers and categorize based on the range
        elif re.fullmatch(digit_pattern, part):
            num = int(part)  # Convert part to integer
            if 1990 <= num <= 2025:
                result_pattern.append('num_label_y')
            elif 1 <= num <= 32:
                result_pattern.append('num_label_dm')
            else:
                result_pattern.append('a number')
            matched = True

        # For any unmatched parts, just add them as is, if they're not empty
        if not matched and part:
            result_pattern.append(part)
    results_pattern=[item for item in result_pattern if item not in char_list]
    return results_pattern




def convert_to_pattern_words(text):
    if pd.isna(text) or text.strip() == '':
        return []
    # Define regex patterns for different components
    month_pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Sept)'
    digit_pattern = r'\d+'
    
    # Initialize the result pattern words list
    result_words = []
    
    # Split the text into parts and match each part with its corresponding word
    parts = re.split(r'(\W+)', text)  # Split by non-word characters
    for part in parts:
        part = part.strip()  # Strip each part to remove leading/trailing whitespace
        
        # Check for month names
        if re.fullmatch(month_pattern, part, re.IGNORECASE):
            result_words.append(part)  # Append the actual month name
        # Check for numbers and categorize based on the range
        elif re.fullmatch(digit_pattern, part):
            num = int(part)  # Convert part to integer
            if 1990 <= num <= 2025:
                result_words.append(part)  # Append the year
            elif 1 <= num <= 32:
                result_words.append(part)  # Append the day or month number
            else:
                result_words.append(part)  # Append the number
        # For any unmatched parts, just add them as is, if they're not empty
        elif part:
            result_words.append(part)
    
    return result_words




def convert_to_pattern_words_without_comma(text):
    char_list=[',',':','.','-','_','–']
    if pd.isna(text) or text.strip() == '':
        return []
    # Define regex patterns for different components
    month_pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Sept)'
    digit_pattern = r'\d+'
    
    # Initialize the result pattern words list
    result_words = []
    
    # Split the text into parts and match each part with its corresponding word
    parts = re.split(r'(\W+)', text)  # Split by non-word characters
    for part in parts:
        part = part.strip()  # Strip each part to remove leading/trailing whitespace
        
        # Check for month names
        if re.fullmatch(month_pattern, part, re.IGNORECASE):
            result_words.append(part)  # Append the actual month name
        # Check for numbers and categorize based on the range
        elif re.fullmatch(digit_pattern, part):
            num = int(part)  # Convert part to integer
            if 1990 <= num <= 2025:
                result_words.append(part)  # Append the year
            elif 1 <= num <= 32:
                result_words.append(part)  # Append the day or month number
            else:
                result_words.append(part)  # Append the number
        # For any unmatched parts, just add them as is, if they're not empty
        elif part:
            result_words.append(part)
            
    results_words=[item for item in result_words if item not in char_list]
    
    return results_words



def filter_specific_labels(parts):
    # Define the labels to keep
    labels_to_keep = {'month_name', 'num_label_dm', 'num_label_y'}
    
    # Initialize the result list
    result = []
    
    for part in parts:
        # Check if the part is one of the specified labels
        if part in labels_to_keep:
            result.append(part)
    
    return result


def filter_specific_words(words, patterns):
    # Define the labels to keep
    labels_to_keep = {'month_name', 'num_label_dm', 'num_label_y'}
    
    # Initialize the result list
    filtered_words = []
    
    # Iterate through the patterns and their corresponding words
    for pattern, word in zip(patterns, words):
        if pattern in labels_to_keep:
            filtered_words.append(word)
    
    return filtered_words
                




                
def dollar_extractor(text):
    # Return 0 if the text is NaN
    if pd.isna(text):
        return 0
    
    # Check if there is a dollar sign in the text
    if '$' in text:
        return 1
    else:
        return 0


# Function to process the text
def process_text(text, singular_map):
    # Make the text lowercase
    text = text.lower()

    # Replace plural words with their singular forms
    for plural, singular in singular_map.items():
        text = text.replace(plural, singular)

    return text


def unit_extracted_for_text(text):
    # Regular expression to find content inside parentheses
    singular_map = {
        "thousands": "thousand",
        "millions": "million",
        "billions": "billion",
        "dollars": "dollar",
        "shares": "share",
        "amounts": "amount"
    }
    
    parenthetical_contents = re.finditer(r'\((.*?)\)', text)
    

    
    # Iterate through all parenthetical matches
    for content in parenthetical_contents:
        inner_text = content.group(1)
        
        # Check for digits and required units
        if re.search(r'\d', inner_text):  # Ignore if digits are present
            continue
        
        if not re.search(r'\b(thousand|million|billion)s?\b', inner_text, re.IGNORECASE):
            continue  # Ignore if no relevant units are found
        
        y=inner_text
        # Split the text using non-alphabetic characters and rejoin with a whitespace
        y = ' '.join(re.split(r'[^a-zA-Z]', y))

        processed_text = process_text(y, singular_map)

        # Search for unit indicators in the processed text
        if any(unit in processed_text for unit in ['in thousand', 'in million', 'in billion']):
            words = processed_text.split()
            initial_size = len(words)
            
            if len(words)>20:
                continue
            # List of words to exclude
            exclude_words = {'in', 'of', 'thousand', 'million', 'billion', 'share', 'per', 'dollar', 'paid', 'price', 'average', 'total', 'number', 'amount', 'data', 'information', 'except','and','are','expressed','reflected'}

            # Remove excluded words
            remaining_words = [word for word in words if word not in exclude_words]
            if len(remaining_words) < 0.2 * initial_size:
                return y.strip()
            if len(words)<=7 and len(remaining_words) < 0.5 * initial_size:
                return y.strip()
    return None





# Function to extract units mentioned in the text below the table
def extract_units_from_after_contents(soup_after):
    singular_map = {
        "thousands": "thousand",
        "millions": "million",
        "billions": "billion",
        "dollars": "dollar",
        "shares": "share",
        "amounts": "amount"
    }

    after_contents = soup_after.contents
    unit_in_after_contents = {}

    for idx, x in enumerate(after_contents):
        y = x.get_text(separator=' ', strip=True)
        
        # Split the text using non-alphabetic characters and rejoin with a whitespace
        y = ' '.join(re.split(r'[^a-zA-Z]', y))

        processed_text = process_text(y, singular_map)

        # Search for unit indicators in the processed text
        if any(unit in processed_text for unit in ['in thousand', 'in million', 'in billion']):
            words = processed_text.split()
            initial_size = len(words)
            
            if len(words)>15:
                continue

            # List of words to exclude
            exclude_words = {'in', 'of', 'thousand', 'million', 'billion', 'share', 'per', 'dollar', 'paid', 'price', 'average', 'total', 'number', 'amount', 'data', 'information', 'except','and','are'}

            # Remove excluded words
            remaining_words = [word for word in words if word not in exclude_words]

            # Check if the remaining words are less than 20% of the initial size
            if len(remaining_words) < 0.2 * initial_size:
                unit_in_after_contents[idx] = y.strip()
            if len(words)<=7:
                unit_in_after_contents[idx] = y.strip()
                

    return unit_in_after_contents





def other_missing_creater(text):
    # Check if the input is None or not a string
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    # Check if the text is one of our custom placeholders for missing data
    if text in ['!p', '!P', '!u', '!d','!s']:
        return text
    
    # Normalize case for simplicity
    text_norm = text.strip().lower()
    
    # Check for common "not applicable" patterns
    if re.match(r'^n[/\-.,]?a$', text_norm):
        return '!o'
    
    # Check for strings consisting only of non-alphanumeric characters
    if re.match(r'^[^a-zA-Z0-9]+$', text_norm):
        unique_chars = set(text_norm)
        if len(unique_chars) <= 2:  # Only one or two unique non-alphanumeric characters
            return '!o'
    
    # If none of the above conditions were met, return the text as is
    return text






def unit_remover(text):
    if pd.isna(text):
        return text  # Return as is if the input is NaN
    
    if not isinstance(text, str):
        return text  # Return as is if it's not a string
    
    original_text = text.strip()
    
    # Update regex pattern to handle unit words attached to numbers
    units_pattern = r'(\b|\d)(thousand|thousands|million|millions|billion|billions)\b'
    
    # Replace unit words with a single space
    text = re.sub(units_pattern, r'\1 ', text, flags=re.IGNORECASE)
    
    # Remove extra whitespaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    # If the resulting text is empty and the original text was not, return a placeholder
    if len(original_text) > 0 and len(text) == 0:
        return '!u'
    
    return text




def general_parenth_remover(text):
    if pd.isna(text):
        return text  # Return as is if the input is NaN
    
    if not isinstance(text, str):
        return text  # Return as is if it's not a string
    
    text = text.strip()
    original_text = text
    
    # Regex pattern to match any content inside parentheses, possibly followed by specific punctuation
    pattern = r'\([^()]*\)\s*([,\/&])?\s*'

    # Use a loop to continuously apply the regex substitution until no more matches are found
    while True:
        new_text = re.sub(pattern, '', text)
        if new_text == text:  # If no change, break the loop
            break
        text = new_text  # Update text with the new changes
    
    # Remove leading/trailing punctuation that might have been left after removing parentheses
    text = re.sub(r'^\s*[,\/&]+\s*|\s*[,\/&]+\s*$', '', text)

    # If removing parentheses empties the text and it was not empty originally, return a placeholder
    if len(original_text) > 0 and len(text.strip()) == 0:
        return '!P'
    
    return text.strip()



def star_remover(text):
    if pd.isna(text):
        return text  # Return as is if the input is NaN
    
    if not isinstance(text, str):
        return text  # Return as is if it's not a string
    
    text = text.strip()
    original_text = text
    
    # Regex pattern to remove all '*' characters
    text = re.sub(r'\*', '', text)
    
    # If removing '*' empties the text and it was not empty originally, return a placeholder
    if len(original_text) > 0 and len(text.strip()) == 0:
        return '!s'
    
    return text.strip()





def single_digit_or_letter_in_parenth_remover(text):
    if pd.isna(text):
        return text  # Return as is if the input is NaN
    
    if not isinstance(text, str):
        return text
    text=text.strip()
    org_text=text
    # Regex pattern to match single digits or letters inside parentheses, possibly followed by specific punctuation
    pattern = r'\(\s*([0-9]|[A-Za-z])\s*\)\s*([,\/&])?\s*'
    
    # Use a loop to continuously apply the regex substitution until no more matches are found
    while True:
        new_text = re.sub(pattern, '', text)
        if new_text == text:  # If no change, break the loop
            break
        text = new_text  # Update text with the new changes
    if len(org_text)>0 and len(text)==0:
        return '!p'
    return text.strip()








def extract_single_digit_or_letter_in_parenth(text):
    if pd.isna(text):
        return [] # Return as is if the input is NaN

    if not isinstance(text, str):
        return []

    text = text.strip()
    # Regex pattern to match single digits or letters inside parentheses
    pattern = r'\(\s*([0-9]|[A-Za-z])\s*\)'

    # Use regex.findall to extract all matches
    extracted_items = re.findall(pattern, text)

    # The extracted_items list will contain the characters found inside parentheses
    return extracted_items










# ************ new functions ********


def fetch_repurchases_html_section(
    filing_url: str,
    section: str = "part2item2",
    api_key_env: str = "SEC_API_KEY",
) -> Optional[str]:
    """
    Fetch the HTML content of a specific section (e.g., 'part2item2')
    from a 10-Q or 10-K filing using the SEC API.

    Parameters
    ----------
    filing_url : str
        The full URL of the filing (e.g., as returned by EDGAR or SEC API).
    section : str, optional
        The section identifier to extract. Defaults to "part2item2".
    api_key_env : str, optional
        Name of the environment variable that stores the SEC API key.
        Defaults to "SEC_API_KEY".

    Returns
    -------
    Optional[str]
        The raw HTML content of the requested section, or `None` if fetching fails.

    Raises
    ------
    ValueError
        If no API key is found in the environment.
    Exception
        Propagates any underlying exception raised by the API.

    Examples
    --------
    >>> html = fetch_repurchases_html_section(
    ...     "https://www.sec.gov/Archives/edgar/data/320193/000032019323000066/aapl-20230930.htm"
    ... )
    >>> html[:100]
    '<html><head>...'

    Notes
    -----
    - The API key should be set as an environment variable **before** calling this function:
        export SEC_API_KEY="your_key_here"
    - This avoids hardcoding secrets in the repository.
    - For GitHub, store the key in `.env` (untracked) or as an Actions secret.
    """
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise ValueError(
            f"Missing API key: please set the environment variable '{api_key_env}'"
        )

    query_api = QueryApi(api_key=api_key)
    extractor_api = ExtractorApi(api_key)

    try:
        html_content = extractor_api.get_section(filing_url, section, "html")
        return html_content
    except Exception as e:
        print(f"[fetch_repurchases_html_section] Error fetching section: {e}")
        return ''








def reset_integer_index_and_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reset the index and/or columns of a DataFrame to consecutive integers 
    starting from zero — but only if they are currently all integers.

    This is useful for normalizing messy tables (e.g., parsed from EDGAR filings)
    where columns or index may accidentally be parsed as integer labels
    rather than actual 0..N ranges.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to normalize.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with reset integer index and/or columns, if applicable.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame([[1, 2], [3, 4]])
    >>> df.columns = [10, 11]
    >>> df.index = [100, 101]
    >>> reset_integer_index_and_columns(df).columns
    RangeIndex(start=0, stop=2, step=1)

    Notes
    -----
    - If columns or index are **not** all integers, they are left unchanged.
    - This function modifies the DataFrame **in-place** and also returns it 
      (so you can chain or assign).
    """
    # Check if all columns are integers
    if all(isinstance(col, int) for col in df.columns):
        df.columns = range(df.shape[1])

    # Check if all index values are integers
    if all(isinstance(idx, int) for idx in df.index):
        df.index = range(df.shape[0])

    return df


def dollar_dropper(text):
    if pd.isna(text):
        return text  # Return as is if the input is NaN
    
    if not isinstance(text, str):
        return text
    org_text=text.strip()
    # Remove any standalone $ within parentheses, potentially surrounded by spaces
    text = re.sub(r'\(\s*\$\s*\)', '', text)
    # Remove all remaining dollar signs
    text = text.replace('$', '')
    # Strip leading and trailing whitespace
    text=text.strip()
    if len(org_text)>0 and len(text)==0:
        return '!d'
    
    return text.strip()




def label_score(text):
    if pd.isna(text):
        return text  # Return as is if the input is NaN
    
    if not isinstance(text, str):
        return np.nan
    
    score=0
    
    if 'repurchase' in text:
        score+=1
    if 'program' in text:
        score+=1
    if 'open' in text:
        score+=1
    if 'employe' in text:
        score-=1
    if 'transaction' in text:
        score-=1
    if 'retir' in text:
        score-=1
    if 'asr' in text:
        score-=1
    if 'accel' in text:
        score-=1
    if 'compen' in text:
        score-=1
        
    if 'opti' in text:
        score-=1
    return score



                        
                        
def convert_to_number(text):
    if pd.isna(text):
        return text  # Return as is if the input is NaN
    
    if not isinstance(text, str):
        return text
    
    try:
        # Attempt to convert directly to float
        return float(text)
    except ValueError:
        # If direct conversion fails, try removing commas and converting again
        try:
            # Remove commas for thousands separators
            return float(text.replace(',', ''))
        except ValueError:
            # Return the original text if all conversions fail
            return text
        
        
        
                
def inner_cell_health_checker(cell):
    if pd.isna(cell):
        return 1  # True for NaN
    
    if not isinstance(cell, str):
        return 1  # True for non-string types
    
    # List of acceptable placeholders
    placeholders = ['!p', '!P', '!d', '!u', '!o','!s']
    
    if cell in placeholders:
        return 1  # True if it's one of our placeholders
    
    return 0  # False for any other string
                         
        
        

def month_to_number(month):
    # Dictionary to map month names and abbreviations to numbers
    months = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9, 'sept':9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12
    }
    
    # Convert the input month to lowercase to ensure case insensitivity
    month = month.lower()
    
    # Return the month number, handling cases where the month is not found
    return months.get(month, None)  # Returns None if the month is not found in the dictionary



def days_in_month(year, month):
    # monthrange returns a tuple (weekday of first day of the month, number of days in month)
    _, num_days = calendar.monthrange(year, month)
    return num_days
        
        
        
        
       
        
def fetch_period_report_date(
    filing_url: str,
    api_key_env: str = "SEC_API_KEY",
) -> Optional[str]:
    
    api_key = os.getenv(api_key_env)
    if not api_key:
        raise ValueError(
            f"Missing API key: please set the environment variable '{api_key_env}'"
        )

    xbrlApi = XbrlApi(api_key)
    


    try:
        xbrl_json = xbrlApi.xbrl_to_json(
            htm_url=filing_url
        )
        
        return xbrl_json['CoverPage']['DocumentPeriodEndDate']
    except Exception as e:
        print(f"Error fetching period_report_date {e}")
        return ''
        
        
        
        
