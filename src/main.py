#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 30 19:00:06 2024

@author: SEC Repurchase Data Extractor Team
"""


from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re

from sec_api import QueryApi
from sec_api import ExtractorApi
from sec_api import RenderApi


from collections import Counter

import calendar

import datetime

import os

import copy 

from utils import *

from dotenv import load_dotenv
load_dotenv()


class ExtractionError(Exception):
    """Custom exception for extraction flow control"""
    def __init__(self, flow_dic, df_output2, message=""):
        self.flow_dic = flow_dic
        self.df_output2 = df_output2
        self.message = message
        super().__init__(self.message)


class RepurchaseExtractor:
    def __init__(self, file_link_filing):
        self.file_link_filing = file_link_filing
        self.flow_dic = {}
        self.df_output2 = pd.DataFrame()
        self.html_content = None
        self.period_report_date = None
        self.period_year = None
        self.table = None
        self.soup_before = None
        self.soup_after = None
    
    def _fetch_html_and_period_data(self):
        """Fetch HTML content and period report date from SEC filing"""
        self.html_content = fetch_repurchases_html_section(self.file_link_filing)
        period_report_str = fetch_period_report_date(self.file_link_filing)
        self.period_report_date = pd.to_datetime(period_report_str, format='%Y-%m-%d')
        self.period_year = self.period_report_date.year
    
    def _identify_and_extract_table(self):
        """Identify the correct table and extract it from HTML"""
        # Check if HTML content is empty
        if len(self.html_content) == 0:
            self.flow_dic['self_term_re'] = 'len_html_zero'
            raise ExtractionError(self.flow_dic, self.df_output2, "No HTML content")
        
        # Define typical words for repurchase tables
        typical_words_list = [
            'paid', 'total', 'part', 'announced', 'shares', 'plans', 'period', 
            'purchases', 'number', 'share', 'under', 'publicly', 'yet', 'price', 
            'programs', 'average', 'may', 'per', 'purchased', 'plan', 'program', 
            'approximate', 'maximum', 'dollar', 'value', 'aggregate', 'except', 'dollars'
        ]
        typical_words_set = set(typical_words_list)
        
        # Parse HTML content
        soup = BeautifulSoup(self.html_content, 'html.parser')
        soup_org2 = copy.deepcopy(soup)
        soup_org2_text = soup_org2.get_text(separator=' ', strip=True)
        
        # Process text with white_word_maker
        try:
            soup_org2_word_list = white_word_maker(soup_org2_text)
        except Exception as e:
            self.flow_dic['error_term_re'] = "white_word_maker_soup_org2_word_num"
            self.flow_dic['error_term_re_e'] = str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"Error processing text: {e}")
        
        # Count tables
        tables = soup.find_all('table')
        num_tables = len(tables)
        self.flow_dic['num_tables'] = num_tables
        
        if num_tables == 0:
            self.flow_dic['self_term_re'] = 'num_tables_zero'
            raise ExtractionError(self.flow_dic, self.df_output2, "No tables found")
        
        # Analyze tables
        tables_length_list = [len(xt) for xt in tables]
        table_lengths = [len(str(table)) for table in tables]
        
        # Create table database
        table_db = pd.DataFrame({
            'len_table': tables_length_list
        }, index=range(len(tables)))
        
        # Analyze each table
        num_rows = []
        num_cols = []
        num_rows2 = []
        num_cols2 = []
        
        for table in tables:
            if len(table.get_text(strip=True)) < 10:
                num_rows.append(0)
                num_cols.append(0)
                num_rows2.append(0)
                num_cols2.append(0)
                continue
                
            table_str = str(table)
            
            try:
                df_table = pd.read_html(table_str)[0]
                df_table.replace("", np.nan, inplace=True)
                df_table_cleaned = df_table.dropna(axis=0, how='all').dropna(axis=1, how='all')
                
                num_rows2.append(df_table_cleaned.shape[0])
                num_cols2.append(df_table_cleaned.shape[1])
                num_rows.append(df_table.shape[0])
                num_cols.append(df_table.shape[1])
                
            except ValueError:
                num_rows.append(0)
                num_cols.append(0)
                num_rows2.append(0)
                num_cols2.append(0)
                continue
        
        # Add analysis results to table_db
        table_db['num_rows'] = num_rows
        table_db['num_cols'] = num_cols
        table_db['num_rows2'] = num_rows2
        table_db['num_cols2'] = num_cols2
        
        # Define filter words
        months_full = ["january", "february", "march", "april", "may", "june", 
                       "july", "august", "september", "october", "november", "december"]
        months_abbr = ["jan", "feb", "mar", "apr", "may", "jun", 
                       "jul", "aug", "sep", "oct", "nov", "dec"]
        month_words = months_full + months_abbr
        unit_words = ["thousand", "million", "billion", "thousands", "millions", "billions"]
        filter_words = month_words + unit_words
        
        # Extract words from each table
        word_lists = []
        for table in tables:
            text = table.get_text(separator=' ', strip=True)
            words = re.split(r'[^a-zA-Z]+', text)
            filtered_words = [word.lower() for word in words if len(word) > 2 and word.lower() not in filter_words]
            word_lists.append(filtered_words)
        
        # Add word analysis to table_db
        table_db['word_list'] = word_lists
        table_db['word_inter_set'] = table_db['word_list'].apply(lambda words: list(set(words) & typical_words_set))
        table_db['word_inter_len'] = table_db['word_inter_set'].apply(len)
        table_db['word_len'] = table_db['word_list'].apply(len)
        
        # Filter tables of interest
        filtered_tables = table_db[(table_db['num_rows2'] >= 4) &
                                   (table_db['num_cols2'] >= 5) &
                                   (table_db['word_inter_len'] >= 8)]
        
        table_indexes = filtered_tables.index.tolist()
        table_id = None
        
        # Determine table of interest
        if not table_indexes:
            self.flow_dic['self_term_re'] = 'no_table_of_interest_found'
            raise ExtractionError(self.flow_dic, self.df_output2, "No table of interest found")
        elif len(table_indexes) == 1:
            table_id = table_indexes[0]
            self.flow_dic['table_of_interest_id'] = table_id
            self.flow_dic['table_of_interest_sit'] = 'unique'
        else:
            max_word_inter_len = filtered_tables['word_inter_len'].max()
            second_max_word_inter_len = filtered_tables['word_inter_len'].nlargest(2).iloc[-1]
            
            if max_word_inter_len >= 1.3 * second_max_word_inter_len:
                table_id = filtered_tables[filtered_tables['word_inter_len'] == max_word_inter_len].index[0]
                self.flow_dic['table_of_interest_id'] = table_id
                self.flow_dic['table_of_interest_sit'] = 'majority_of_word_inter_len'
            else:
                self.flow_dic['self_term_re'] = 'multiple_tables_of_interest'
                raise ExtractionError(self.flow_dic, self.df_output2, "Multiple tables of interest found")
        
        if table_id is not None:
            print(f"Table ID set to: {table_id}")
        
        # Handle other significant tables
        other_significant_tables = table_db[(table_db['num_cols2'] >= 3) & (table_db['word_len'] > 6)]
        other_table_indexes = other_significant_tables.index.tolist()
        
        if table_id in other_table_indexes:
            other_table_indexes.remove(table_id)
        
        print("Indexes of other significant tables (excluding the primary table of interest):", other_table_indexes)
        
        # Process other significant tables
        other_id = None
        other_dum = 0
        other_loc = None
        
        self.flow_dic['num_other_sig_tables'] = len(other_table_indexes)
        
        if len(other_table_indexes) == 0:
            print("No other significant table was found.")
        elif len(other_table_indexes) > 1:
            print("There is more than one other significant table.")
            min_other_table_indexes = min(other_table_indexes)
            if min_other_table_indexes > table_id:
                other_id = min_other_table_indexes
            else:
                self.flow_dic['self_term_re'] = 'multiple_other_sig_tables'
                raise ExtractionError(self.flow_dic, self.df_output2, "Multiple other significant tables found")
        else:
            other_id = other_table_indexes[0]
            self.flow_dic['unique_other_sig_table_id'] = other_id
            other_dum = 1
            if other_id < table_id:
                other_loc = 0
            else:
                other_loc = 1
            print(f"Other significant table found at index: {other_id}, location relative to table of interest: {'before' if other_loc == 0 else 'after'}")
        
        # Handle other table removal if needed
        if other_dum == 1:
            soup_str = str(soup)
            other_table = tables[other_id]
            table_html = str(other_table)
            
            other_table_start = None
            other_table_end = None
            
            try:
                start_pattern = re.escape(table_html[:700])
                start_match = re.search(start_pattern, soup_str)
                
                if start_match:
                    other_table_start = start_match.start()
                    other_table_end = other_table_start + len(table_html)
                else:
                    print("No match found for the table in the HTML content.")
                    self.flow_dic['self_term_re'] = 'start_match_of_sig_wasnt_found'
                    raise ExtractionError(self.flow_dic, self.df_output2, "Could not locate other significant table")
                    
            except Exception as e:
                print("Error finding the table:", e)
                self.flow_dic['error_term_re'] = "Error_finding_the_sig_table"
                self.flow_dic['error_term_re_e'] = str(e)
                raise ExtractionError(self.flow_dic, self.df_output2, f"Error finding significant table: {e}")
            
            if other_table_start is not None and other_table_end is not None:
                print(f"Start of the other table: {other_table_start}, End of the other table: {other_table_end}")
            else:
                print("Failed to locate the other table in the document.")
                self.flow_dic['self_term_re'] = 'failed_to_locate_sig_table'
                raise ExtractionError(self.flow_dic, self.df_output2, "Failed to locate other significant table")
        
        # Remove other table if needed
        soup_org = soup
        if other_dum == 1 and other_table_start and other_table_end:
            if other_loc == 1:
                soup_str = soup_str[:other_table_start]
            elif other_loc == 0:
                soup_str = soup_str[other_table_end+1:]
            
            soup = BeautifulSoup(soup_str, 'html.parser')
            print("Updated HTML document with the other table removed.")
        
        # Locate the main table
        soup_str = str(soup)
        table = tables[table_id]
        table_html = str(table)
        
        table_start = None
        table_end = None
        
        try:
            start_pattern = re.escape(table_html[:700])
            start_match = re.search(start_pattern, soup_str)
            
            if start_match:
                table_start = start_match.start()
                table_end = table_start + len(table_html)
            else:
                print("No match found for the table in the HTML content.")
                self.flow_dic['self_term_re'] = 'start_match_of_table_wasnt_found'
                raise ExtractionError(self.flow_dic, self.df_output2, "Could not locate main table")
        except Exception as e:
            print("Error finding the table:", e)
            self.flow_dic['error_term_re'] = "Error_finding_the_table"
            self.flow_dic['error_term_re_e'] = str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"Error finding main table: {e}")
        
        if table_start is not None and table_end is not None:
            print(f"Start of the table: {table_start}, End of the table: {table_end}")
        else:
            print("Failed to locate the table in the document.")
            self.flow_dic['self_term_re'] = 'failed_to_locate_table'
            raise ExtractionError(self.flow_dic, self.df_output2, "Failed to locate main table")
        
        # Extract soup before and after table
        parser_label = 'html.parser'
        text_before_table = soup_str[:table_start]
        text_after_table = soup_str[table_end:]
        
        self.soup_before = BeautifulSoup(text_before_table, parser_label)
        self.soup_after = BeautifulSoup(text_after_table, parser_label)
        
        # Preprocess and set the table
        self.table = preprocess_html(table)
    


    def _preprocess_table(self):
        """Preprocess the table"""
        # Process the identified table
        df = pd.read_html(str(self.table))[0]
        
           
        # Check if all columns and rows are integers and reset index/columns if true
        df=reset_integer_index_and_columns(df)

        
        # Check if the columns need to be reset (by verifying if the current names are not already integers)
        expected_columns = list(range(df.shape[1]))
        
        if list(df.columns) != expected_columns:
            stripped_columns = [re.sub(r'\.\d+', '', col) for col in df.columns]
            # Create a new DataFrame row from the stripped column names
            new_row = pd.DataFrame([stripped_columns], columns=expected_columns)

            # Reset the column names to integer sequence
            df.columns = expected_columns

            # Concatenate the new row with the original df
            df = pd.concat([new_row, df], ignore_index=True)
            df.replace("", np.nan, inplace=True)
            
            # Identify columns with the same name and check for all NaN values
            for name in set(stripped_columns):
                
                cols_to_check = [idx for idx, col in enumerate(stripped_columns) if col == name]
                all_nan_columns = [col for col in cols_to_check if df.loc[1:, col].isna().all()]
                non_nan_columns = [col for col in cols_to_check if not df.loc[1:, col].isna().all()]
                
            
                # Drop all NaN columns except one (if multiple are all NaN)
                if len(non_nan_columns)>0 and len(all_nan_columns) > 0:
                    df.drop(all_nan_columns, axis=1, inplace=True)
                    

                if len(non_nan_columns)==0 and len(all_nan_columns) > 1:
                    # Drop all but one
                    df.drop(all_nan_columns[1:], axis=1, inplace=True)
        
        # Check if all columns and rows are integers and reset index/columns if true
        df=reset_integer_index_and_columns(df)
        
        self.flow_dic['df_st1_shape0']=df.shape[0]
        self.flow_dic['df_st1_shape1']=df.shape[1]
        
        df_cop_in=df.copy()

        try:
            df=df.map(convert_to_string_if_not_nan)
        except Exception as e:
            self.flow_dic['error_term_re']="convert_to_string_if_not_nan"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"convert_to_string_if_not_nan: {e}")
            
        try:
            df=df.map(unicode_text_cleaner)
        
        except Exception as e:
            self.flow_dic['error_term_re']="unicode_text_cleaner"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"unicode_text_cleaner: {e}")
        
        
        try:
            df=df.map(convert_and_parenthesize_superscripts)
        
        except Exception as e:
            self.flow_dic['error_term_re']="convert_and_parenthesize_superscripts"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"convert_and_parenthesize_superscripts: {e}")
        
        
        try:
            df=df.map(bracket_to_paranth)
        
        except Exception as e:
            self.flow_dic['error_term_re']="bracket_to_paranth"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"bracket_to_paranth: {e}")
        
        try:
            df=df.map(check_issuer_in_text)
        
        except Exception as e:
            self.flow_dic['error_term_re']="check_issuer_in_text"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"check_issuer_in_text: {e}")
        
        # Replace empty strings with NaN
        df.replace("", np.nan, inplace=True)
        # Remove all NaN columns and rows
        df = df.dropna(axis=1, how='all')
        df = df.dropna(axis=0, how='all')
        
        # Check if all columns and rows are integers and reset index/columns if true
        df=reset_integer_index_and_columns(df)

        

        try:
            df=df.map(para_whitespace_stripper)
        
        except Exception as e:
            self.flow_dic['error_term_re']="para_whitespace_stripper"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"para_whitespace_stripper: {e}")
        

        
        # Assuming df is your original DataFrame and check_healthy_parentheses is defined as above
        

        try:
            
            df=df.map(three_zero_to_thousand)
        
        except Exception as e:
            self.flow_dic['error_term_re']="three_zero_to_thousand"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"three_zero_to_thousand: {e}")

        try:
            
            df=df.map(dollar_sign_to_dollar_word)
        
        except Exception as e:
            self.flow_dic['error_term_re']="dollar_sign_to_dollar_word"
            self.flow_dic['error_term_re_e']=str(e)
            raise ExtractionError(self.flow_dic, self.df_output2, f"dollar_sign_to_dollar_word: {e}")

        return df


    def extract(self):
        """Main extraction method - orchestrates the entire process"""
        try:
            self.flow_dic['self_term_re'] = np.nan
            self.flow_dic['error_term_re_e'] = np.nan

            # Fetch HTML content and period data
            self._fetch_html_and_period_data()
            
            # Identify and extract the table
            self._identify_and_extract_table()

            df=self._preprocess_table()
            
          
            period_col_span=[]
            period_col_start_cand=None
            period_col_end_cand=None
            
            # Initialize variable to store the row number with "total"
            total_row_temp = None
            
            # Iterate from the last row to the first row
            for i in range(df.shape[0] - 1, -1, -1):
                if pd.notna(df.iloc[i, 0]) and "total" in df.iloc[i, 0].lower():
                    total_row_temp = i
                    break
                
            temp=None 
            
            if total_row_temp:
                temp=total_row_temp+1 
            else:
                temp=df.shape[0]
            
            
            for i in range (df.shape[1]):
                if df.iloc[:temp, 0].equals(df.iloc[:temp, i]):
                    period_col_span.append(i)
                    
            
            period_col_start_cand=min(period_col_span)
            period_col_end_cand=max(period_col_span)
            
         
            
            try:
              
              df_reduced=df.map(text_reducer)
            
            except Exception as e:
                self.flow_dic['error_term_re']="text_reducer"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
   
            df_reduced.replace("", np.nan, inplace=True)
       
            reduced_stat=0
            row_id_with_four_uniques=None
            # Assuming df_reduced is your DataFrame
            unique_values_dict = {}
            max_length=None
            rows_with_four_uniques=[]
            all_greater_than_16=None
            at_least_two_greater_than_4=None
            # Iterate over the DataFrame rows, skip the first column
            for index, row in df_reduced.iloc[:, period_col_end_cand+1:].iterrows():
                # Store the unique non-NaN values of each row in the dictionary
                unique_values_dict[index] = list(row.dropna().unique())
            
            potential_footnote_rows = []

            # Loop through the unique_values_dict
            for index, values in unique_values_dict.items():
                if len(values) <= 2:  # Check if the row has 2 or fewer unique non-NaN entries
                    if any(len(str(item)) > 80 for item in values):  # Check if any item's length exceeds 120 characters
                        potential_footnote_rows.append(index)
            potential_footnote_rows=[item for item in potential_footnote_rows if item>4]
            if len(potential_footnote_rows)>0:
                
                
                potential_footnote_rows_min_row=min(potential_footnote_rows)
                # Create a new DataFrame containing potential footnotes from the minimum index onward
                df_potential_footnote_in_table = df.iloc[potential_footnote_rows_min_row:]

                # Drop the same rows from the original DataFrame
                df = df.drop(df.index[potential_footnote_rows_min_row:])
                # Replace empty strings with NaN
                df.replace("", np.nan, inplace=True)
                # Remove all NaN columns and rows
                df = df.dropna(axis=1, how='all')
                df = df.dropna(axis=0, how='all')
            
                # Check if all columns and rows are integers and reset index/columns if true
                are_all_columns_integers = all(isinstance(col, int) for col in df.columns)
                if are_all_columns_integers:
                    df.columns = range(df.shape[1])
            
                are_all_rows_integers = all(isinstance(row, int) for row in df.index)
                if are_all_rows_integers:
                    df.index = range(df.shape[0])
                    

                df_reduced=df.map(text_reducer)
                
                df_reduced.replace("", np.nan, inplace=True)
                
                
                
                reduced_stat=0
                row_id_with_four_uniques=None
                # Assuming df_reduced is your DataFrame
                unique_values_dict = {}
                max_length=None
                rows_with_four_uniques=[]
                all_greater_than_16=None
                at_least_two_greater_than_4=None
                # Iterate over the DataFrame rows, skip the first column
                for index, row in df_reduced.iloc[:, period_col_end_cand+1:].iterrows():
                    # Store the unique non-NaN values of each row in the dictionary
                    unique_values_dict[index] = list(row.dropna().unique())

            # Find the maximum length among the values in the dictionary
            max_length = max(len(values) for values in unique_values_dict.values())
            
            # Check if the maximum length is 4
            if max_length == 4:
                reduced_stat+=1
                print("The maximum length of unique values in any row is 4.")
            else:
                print(f"The maximum length of unique values in any row is {max_length}.")
            
            # Find rows with exactly four unique values
            rows_with_four_uniques = [idx for idx, vals in unique_values_dict.items() if len(vals) == 4]
            
            # Check if only one row has exactly four unique values and return the row ID
            if len(rows_with_four_uniques) == 1:
                row_id_with_four_uniques = rows_with_four_uniques[0]
                reduced_stat+=1
                print(f"Only one row, index {row_id_with_four_uniques}, has exactly four unique values.")
                
                # Retrieve the list of strings associated with the row ID
                unique_strings = unique_values_dict[row_id_with_four_uniques]
                
                # Calculate the length of each string in the list
                lengths_of_strings = [len(s) for s in unique_strings]
                
                # Print the lengths
                print(f"Lengths of the strings in row {row_id_with_four_uniques}: {lengths_of_strings}")
                # Assuming lengths_of_strings is defined and contains four lengths
                all_greater_than_16 = all(length > 16 for length in lengths_of_strings)
                at_least_two_greater_than_40 = sum(length > 35 for length in lengths_of_strings) >= 2
                
                if all_greater_than_16 and at_least_two_greater_than_40:
                    reduced_stat+=1
                    print(f"All lengths are greater than 16 and at least two are greater than 40 in row {row_id_with_four_uniques}.")
                    
                else:
                    if not all_greater_than_16:
                        print("Not all lengths are greater than 16.")
                    if not at_least_two_greater_than_40:
                        print("There are not at least two lengths greater than 40.")
            
            else:
                if len(rows_with_four_uniques) > 1:
                    print("Multiple rows have exactly four unique values.")
                else:
                    print("No row has exactly four unique values.")
            
            self.flow_dic['first_reduced_stat']=reduced_stat
            
            if reduced_stat < 3:
                
                # Initialize variables to keep track of the first non-empty row and the first empty row after non-empty rows
                first_nonempty_row = None
                first_empty_row_after_nonempty = None
                
                # Iterate through unique_values_dict to find the first empty row after non-empty rows
                for index, values in unique_values_dict.items():
                    if len(values) > 0:
                        if first_nonempty_row is None:
                            first_nonempty_row = index
                    elif first_nonempty_row is not None:
                        first_empty_row_after_nonempty = index
                        break
            
                # Find the first row that has an empty list in unique_values_dict
                first_empty_row = first_empty_row_after_nonempty
                last_nonempty_row = first_empty_row - 1
                
            
                # Find the first row in period_col_end_cand that has a non-NaN value in df_reduced
                first_non_nan_row_in_period_col = next((index for index, value in df_reduced.iloc[:, period_col_end_cand].items() if not pd.isna(value)), None)
                
                print(f"First row with an empty list: {first_empty_row}")
                print(f"First non-NaN row in period column: {first_non_nan_row_in_period_col}")
                print(f"First row with a non-empty list: {first_nonempty_row}")
                
                # Calculate last_index
                if first_empty_row is not None and first_non_nan_row_in_period_col is not None:
                    last_index = min(first_non_nan_row_in_period_col, first_empty_row - 1)
                    
                    if last_index is not None and last_nonempty_row <= last_index + 1:
                        for col in df.columns[period_col_end_cand + 1:]:
                            merged_text = " ".join(filter(None, df.loc[first_nonempty_row:last_index, col].dropna().tolist()))
                            df.loc[last_index, col] = merged_text
                        
                        # If last_nonempty_row is greater than last_index, merge text from last_nonempty_row
                        if last_nonempty_row > last_index:
                            for col in df.columns[period_col_end_cand + 1:]:
                                additional_text = df.loc[last_nonempty_row, col]
                                if not pd.isna(additional_text):
                                    df.loc[last_index, col] = df.loc[last_index, col] + " " + additional_text
            
                        # Set all merged rows to np.nan except for the last_index row
                        for col in df.columns[period_col_end_cand + 1:]:
                            df.loc[first_nonempty_row:last_index-1, col] = np.nan
                            if last_nonempty_row > last_index:
                                df.loc[last_nonempty_row, col] = np.nan
            
            
            
                # Replace empty strings with NaN
                df.replace("", np.nan, inplace=True)
                # Remove all NaN columns and rows
                df = df.dropna(axis=1, how='all')
                df = df.dropna(axis=0, how='all')
            
                # Check if all columns and rows are integers and reset index/columns if true
                df=reset_integer_index_and_columns(df)
                
                

                
                period_col_span=[]
                period_col_start_cand=None
                period_col_end_cand=None
                for i in range (df.shape[1]):
                    if df.iloc[:, 0].equals(df.iloc[:, i]):
                        period_col_span.append(i)
                        
                
                period_col_start_cand=min(period_col_span)
                period_col_end_cand=max(period_col_span)
                
                
                df_reduced=df.map(text_reducer)
                
                df_reduced.replace("", np.nan, inplace=True)
                
          
                
                reduced_stat=0
                row_id_with_four_uniques=None
                # Assuming df_reduced is your DataFrame
                unique_values_dict = {}
                max_length=None
                rows_with_four_uniques=[]
                all_greater_than_16=None
                at_least_two_greater_than_4=None
                # Iterate over the DataFrame rows, skip the first column
                for index, row in df_reduced.iloc[:, period_col_end_cand+1:].iterrows():
                    # Store the unique non-NaN values of each row in the dictionary
                    unique_values_dict[index] = list(row.dropna().unique())
                
     
                # Find the maximum length among the values in the dictionary
                max_length = max(len(values) for values in unique_values_dict.values())
                
                # Check if the maximum length is 4
                if max_length == 4:
                    reduced_stat+=1
                    print("The maximum length of unique values in any row is 4.")
                else:
                    print(f"The maximum length of unique values in any row is {max_length}.")
                
                # Find rows with exactly four unique values
                rows_with_four_uniques = [idx for idx, vals in unique_values_dict.items() if len(vals) == 4]
                
                # Check if only one row has exactly four unique values and return the row ID
                if len(rows_with_four_uniques) == 1:
                    row_id_with_four_uniques = rows_with_four_uniques[0]
                    reduced_stat+=1
                    print(f"Only one row, index {row_id_with_four_uniques}, has exactly four unique values.")
                    
                    # Retrieve the list of strings associated with the row ID
                    unique_strings = unique_values_dict[row_id_with_four_uniques]
                    
                    # Calculate the length of each string in the list
                    lengths_of_strings = [len(s) for s in unique_strings]
                    
                    # Print the lengths
                    print(f"Lengths of the strings in row {row_id_with_four_uniques}: {lengths_of_strings}")
                    # Assuming lengths_of_strings is defined and contains four lengths
                    all_greater_than_16 = all(length > 16 for length in lengths_of_strings)
                    at_least_two_greater_than_40 = sum(length > 40 for length in lengths_of_strings) >= 2
                    
                    if all_greater_than_16 and at_least_two_greater_than_40:
                        reduced_stat+=1
                        print(f"All lengths are greater than 16 and at least two are greater than 40 in row {row_id_with_four_uniques}.")
                        
                    else:
                        if not all_greater_than_16:
                            print("Not all lengths are greater than 16.")
                        if not at_least_two_greater_than_40:
                            print("There are not at least two lengths greater than 40.")
                
                else:
                    if len(rows_with_four_uniques) > 1:
                        print("Multiple rows have exactly four unique values.")
                    else:
                        print("No row has exactly four unique values.")
                

            
            self.flow_dic['second_reduced_stat']=reduced_stat
            
            if reduced_stat<3:
                self.flow_dic['self_term_re']='reduced_less_than_3'
                return (self.flow_dic,self.df_output2)

            
            if reduced_stat==3:
                header_row_id_cand=row_id_with_four_uniques
                

            initial_header_zero_dum = 1
            initial_header_id = None
            df_top_left_overs = pd.DataFrame()
            if reduced_stat < 3:
                print("We were not able to come up with a header_row candidate.")
            else:
                if header_row_id_cand > 0:
                    above_row = df.iloc[header_row_id_cand - 1]
                    initial_header_zero_dum = 0
                    initial_header_id = header_row_id_cand
                    # Create a new DataFrame with the rows above the candidate header row
                    if all(above_row.apply(check_single_digit_or_letter)):
                        merged_header = df.iloc[header_row_id_cand - 1:header_row_id_cand + 1].apply(lambda x: ' '.join(x.dropna()), axis=0)
                        df.iloc[header_row_id_cand] = merged_header
                        df = df.drop(header_row_id_cand - 1).reset_index(drop=True)
                        print("Merged row above with header row candidate.")
                        # After merging, check if header_row_id_cand is now the first row
                        if header_row_id_cand > 1:
                            df_top_left_overs = df.iloc[:header_row_id_cand - 1].copy()
                            # Drop the rows above the candidate header row from the original DataFrame
                            df = df.iloc[header_row_id_cand - 1:].reset_index(drop=True)
                            print("DataFrame above the header row has been moved to df_top_left_overs and df has been reindexed.")
                    else:
                        df_top_left_overs = df.iloc[:header_row_id_cand].copy()
                        # Drop the rows above the candidate header row from the original DataFrame
                        df = df.iloc[header_row_id_cand:].reset_index(drop=True)
                        print("DataFrame above the header row has been moved to df_top_left_overs and df has been reindexed.")
            
                    # Update the candidate header row id to 0 since we've reindexed the DataFrame
                    header_row_id_cand = 0
                else:
                    print("The candidate header row is already the first row. No changes made.")
            
            header_id = header_row_id_cand
            
            
            top_left_any_unit_found=0
            top_left_units=None
            # Check if df_top_left_overs is not empty
            if not df_top_left_overs.empty:
                # Reset the row indexes
                df_top_left_overs = df_top_left_overs.reset_index(drop=True)
                
                # Get the last row
                last_row_top_left_over = df_top_left_overs.iloc[-1].copy()
                last_row_top_left_over_unit=last_row_top_left_over.map(unit_extractor)
                last_row_top_left_over_unit_analysed=last_row_top_left_over_unit.map(unit_analyser)
                l=last_row_top_left_over_unit_analysed.dropna().tolist()
                s=list(set(l))
                
                if len(s)>0:
                    top_left_any_unit_found=1
                    top_left_units=s
                    
                    
                    
                    
            
                
                            

            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])

            # Merging out-of-range footnotes to the proper cell  
            for i in range(df.shape[1]-1):
                if pd.isna(df.iloc[0, i+1]) and pd.notna(df.iloc[0, i]):
                    # Check the entire column to see if all entries are single-digit or letter footnotes
                    if all(df.iloc[:, i+1].apply(check_single_digit_or_letter)):
                        # If all values are valid footnotes, proceed to merge them to the left
                        for index, value in df.iloc[:, i+1].items():
                            if pd.notna(value):  # Check if there's a value to merge
                                if pd.isna(df.iloc[index, i]):
                                    df.iloc[index, i] = value  # Directly assign if left cell is NaN
                                else:
                                    df.iloc[index, i] += ' ' + value  # Concatenate otherwise
            
                                # Clear the value in the right column after transferring
                                df.at[index, df.columns[i+1]] = np.nan
            
            
            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])
            

            
            # Merging out-of-range footnotes to the proper cell
            for i in range(df.shape[1] - 1):
                # Check if the first row of column i+1 is a footnote and column i has a regular header
                if check_single_digit_or_letter(df.iloc[0, i + 1]) and not check_single_digit_or_letter(df.iloc[0, i]):
                    # Check if the entire column i+1 contains only footnotes
                    if all(df.iloc[:, i + 1].apply(check_single_digit_or_letter)):
                        # Merge footnotes to the left column
                        for index, value in df.iloc[:, i + 1].items():
                            if pd.notna(value):  # Check if there's a value to merge
                                if pd.isna(df.iloc[index, i]):
                                    df.iloc[index, i] = value  # Directly assign if left cell is NaN
                                else:
                                    df.iloc[index, i] += ' ' + value  # Concatenate otherwise
            
                                # Clear the value in the right column after transferring
                                df.at[index, df.columns[i + 1]] = np.nan
            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])
            

            
            # Assuming 'df' is your DataFrame and 'header_id' identifies the header row
            

            
            for i in range(1, df.shape[1]):  # Start from 1 since we're looking to the left
                # Check columns where the current has a header and the previous one does not
                if pd.notna(df.iloc[header_id, i]) and pd.isna(df.iloc[header_id, i-1]):
                    # Apply check_only_dollar_sign to the entire left column and check all are True
                    if all(df.iloc[:, i-1].apply(check_only_dollar_sign)):
                        # Merge dollar signs to the right if applicable
                        for index in df.index:
                            value = df.iloc[index, i-1]
                            if pd.notna(value):  # Check if there's a dollar sign to merge
                                if pd.isna(df.iloc[index, i]):
                                    df.iloc[index, i] = value.strip()  # Assign directly if right cell is NaN
                                else:
                                    df.iloc[index, i] = value.strip() + ' ' + df.iloc[index, i]  # Prepend value
            
                        # Clear the values in the left column after transferring
                        df.iloc[:, i-1] = np.nan
            
            # Optional: Cleaning up now empty columns if needed
            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])
  
            #merging scatter information related to one column into one column (the one to the rightest)
            for i in range(df.shape[1]-1):
                if df.iloc[0, i] == df.iloc[0, i+1] and pd.notna(df.iloc[0, i]) :
                    for index, value in df.iloc[1:, i].items():
                        value1=df.iloc[index, i+1]
                        if pd.notna(value) and pd.notna(value1):
                            value=value.strip()
                            value1=value1.strip()
                            if value not in value1 and value1 not in value:
                                value1=value + ' ' + value1 
                                df.at[index, i] = np.nan
                                df.at[index, i+1]=value1
                                continue
                            if value in value1:
                                df.at[index, i] = np.nan
                                continue 
                        
                        if pd.notna(value) and pd.isna(value1):
                            df.at[index, i] = np.nan
                            df.at[index, i+1]=value.strip()
                            continue
            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])
            

            #merging scatter information related to one column into one column (the one to the rightest)
            for i in range(df.shape[1]-1):
                if (pd.isna(df.iloc[0, i+1]) and pd.isna(df.iloc[0, i])) :
                    for index, value in df.iloc[1:, i].items():
                        value1=df.iloc[index, i+1]
                        if pd.notna(value) and pd.notna(value1):
                            value=value.strip()
                            value1=value1.strip()
                            if value not in value1 and value1 not in value:
                                value1=value + ' ' + value1 
                                df.at[index, i] = np.nan
                                df.at[index, i+1]=value1
                                continue
                            if value in value1:
                                df.at[index, i] = np.nan
                                continue 
                        
                        if pd.notna(value) and pd.isna(value1):
                            df.at[index, i] = np.nan
                            df.at[index, i+1]=value.strip()
                            continue

            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])
            
            
            try:
               df=df.map(para_whitespace_stripper)
            
            except Exception as e:
                self.flow_dic['error_term_re']="para_whitespace_stripper"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
   
            # now let's drop duplicate columns:

            # Apply the function
            df.replace("", np.nan, inplace=True)
            df = drop_duplicate_columns(df)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])

            # Merging out-of-range footnotes to the proper cell  
            for i in range(df.shape[1]-1):
                if pd.isna(df.iloc[0, i+1]) and pd.notna(df.iloc[0, i]):
                    # Check the entire column to see if all entries are single-digit or letter footnotes
                    if all(df.iloc[:, i+1].apply(check_single_digit_or_letter)):
                        # If all values are valid footnotes, proceed to merge them to the left
                        for index, value in df.iloc[:, i+1].items():
                            if pd.notna(value):  # Check if there's a value to merge
                                if pd.isna(df.iloc[index, i]):
                                    df.iloc[index, i] = value  # Directly assign if left cell is NaN
                                else:
                                    df.iloc[index, i] += ' ' + value  # Concatenate otherwise
            
                                # Clear the value in the right column after transferring
                                df.at[index, df.columns[i+1]] = np.nan
            
            
            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])
            

            
            # Merging out-of-range footnotes to the proper cell
            for i in range(df.shape[1] - 1):
                # Check if the first row of column i+1 is a footnote and column i has a regular header
                if check_single_digit_or_letter(df.iloc[0, i + 1]) and not check_single_digit_or_letter(df.iloc[0, i]):
                    # Check if the entire column i+1 contains only footnotes
                    if all(df.iloc[:, i + 1].apply(check_single_digit_or_letter)):
                        # Merge footnotes to the left column
                        for index, value in df.iloc[:, i + 1].items():
                            if pd.notna(value):  # Check if there's a value to merge
                                if pd.isna(df.iloc[index, i]):
                                    df.iloc[index, i] = value  # Directly assign if left cell is NaN
                                else:
                                    df.iloc[index, i] += ' ' + value  # Concatenate otherwise
            
                                # Clear the value in the right column after transferring
                                df.at[index, df.columns[i + 1]] = np.nan
            df.replace("", np.nan, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])


            # here I am assuming that first row is the header_row    

            
            columns_header={}
            
            for i in range(0,df.shape[1]):
                h=df.iloc[0,i]
                if h not in columns_header.keys():
                    columns_header[h]=[]
                columns_header[h].append(i) 
            
            unique_headers=set(list(columns_header.keys()))
            
            
            columns_to_keep={}
            
            for k in columns_header.keys():
               if k not in columns_to_keep.keys():
                    columns_to_keep[k]=[]
               v=columns_header[k]
               
               if len(v)==1:
                   columns_to_keep[k].append(v[0])
               if len(v)>1:
            
                   for j in range(len(v)):
                       bo=df.iloc[1:,v[j]].isna().all()
                       if not bo:
                           columns_to_keep[k].append(v[j])
                       if j==len(v)-1 and len(columns_to_keep[k])==0:
                           columns_to_keep[k].append(v[j])
                      
             
            if set(list(columns_to_keep.keys()))==unique_headers:
                for item, va in columns_to_keep.items():
                    if len(va)>1:
                        va_max=max(va)
                        columns_to_keep[item]=[va_max]
                    
                    
                l=list(columns_to_keep.values())
                flat_l=[item for sublist in l for item in sublist] 
                columns_to_keep_list=flat_l
                
            if 0 not in columns_to_keep_list and np.nan in columns_header.keys():
                if 0 in columns_header[np.nan]:
                    columns_to_keep_list.append(0)
                
            columns_to_keep_list.sort()
            #columns_to_keep_list.insert(0, 0) 

            df = df[columns_to_keep_list]
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])

        

            # let's take care of columns with no names in the first row 
            # Create dictionary to store columns with no header and their unique values
            no_name_cols_info = {}
            
            # Iterate through columns to find those with NaN in the header row
            for col in df.columns:
                if pd.isna(df.iloc[0, col]):
                    if col==0:
                        continue 
                    # Collect unique values in the column (excluding the header row)
                    unique_values = set(df.iloc[1:, col].dropna())
                    no_name_cols_info[col] = unique_values
            
            
            # Create a set to store unique values in columns with names
            unique_info_in_name_cols = set()
            
            # Iterate through columns to find those with a name in the header row
            for col in df.columns:
                if pd.notna(df.iloc[0, col]):
                    if col==0:
                        continue
                    # Add unique values in the column (excluding the header row) to the set
                    unique_values = set(df.iloc[1:, col].dropna())
                    unique_info_in_name_cols.update(unique_values)
            
            
            # Create a list to store columns to drop
            no_name_col_to_drop = []
            
            # Iterate over no_name_cols_info to check for exclusive information
            for col, values in no_name_cols_info.items():
                if values.issubset(unique_info_in_name_cols):
                    no_name_col_to_drop.append(col)
                    
                    
            
            df.drop(columns=no_name_col_to_drop, inplace=True)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = df.reset_index(drop=True)
            df.columns = range(df.shape[1])
            
            
            # Create a list to store columns with no name that survived
            no_name_cols_with_excl_info = []
            
            # Iterate through columns to find those with NaN in the header row
            for col in df.columns:
                if pd.isna(df.iloc[0, col]):
                    if col==0:
                        continue 
                    no_name_cols_with_excl_info.append(col)
                    
                    
            
            # Cut columns with no name that contain exclusive information and put them in a new DataFrame
            if no_name_cols_with_excl_info:
                df_no_name_cols_with_excl_info = df[no_name_cols_with_excl_info]
                df.drop(columns=no_name_cols_with_excl_info, inplace=True)
                df = df.dropna(axis=1, how='all')
                df = df.dropna(axis=0, how='all')
                df = df.reset_index(drop=True)
                df.columns = range(df.shape[1])
                
            else:
                df_no_name_cols_with_excl_info = pd.DataFrame()
            
                            
            try:
               # Apply check_healthy_parentheses function to each element in the DataFrame
               df_health_parenth = df.map(check_healthy_parentheses)
            
            except Exception as e:
                self.flow_dic['error_term_re']="check_healthy_parentheses"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2) 
            
            # Calculate the minimum value in df_health_parenth
            min_value = df_health_parenth.min().min()  # This traverses the entire DataFrame
            
            # Check for unhealthy parentheses
            if min_value == -1:
                print("Unhealthy parenthesis is found.")
                self.flow_dic['self_term_re']='Unhealthy_parenthesis_found'
                return (self.flow_dic,self.df_output2)
            else:
                print("No unhealthy parenthesis found.")
            
            # &&&&&&&& check if the shape is good enough.
            

            
            try:
              
              df_reduced2=df.map(text_reducer2)
            
            except Exception as e:
                self.flow_dic['error_term_re']="text_reducer2"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2) 
            
            
            
            df_reduced2.replace("", np.nan, inplace=True)
            
    
            try:
              
              df_reduced2_words=df_reduced2.map(white_word_maker)
            
            except Exception as e:
                self.flow_dic['error_term_re']="white_word_maker"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
            

            # Initialize df_col_roles with the same number of columns as df_reduced2_words and two rows
            df_col_roles = pd.DataFrame(index=[0, 1], columns=range(1,df_reduced2_words.shape[1]))
            
            # Populate the first row with sets of words from df_reduced2_words
            for col in df_reduced2_words.columns[1:]:
                # Create a set of words for each column in df_reduced2_words
                word_set = set()
                for words in df_reduced2_words[col]:
                    word_set.update(words)
                # Assign the set to the first row of df_col_roles
                df_col_roles.at[0, col] = word_set
                
                
            # Define the 'price' role set
            price_set = {'price', 'average'}
            
            # Initialize a list to keep track of columns with the 'price' role
            price_cols = []
            
            # Iterate over the columns to check for the 'price' role
            for col in df_col_roles.columns:
                # Get the set of words for the current column
                words_set = df_col_roles.at[0, col]
                
                # Calculate the intersection with the price_set
                intersection = words_set.intersection(price_set)
                
                # Check if the intersection is non-empty
                if intersection:
                    # Append the column to the price_cols list
                    price_cols.append(col)
            
            # If we found exactly one column with the 'price' role, assign 'price' to the second row
            if len(price_cols) == 1:
                df_col_roles.at[1, price_cols[0]] = 'price'
            
                
            # Define the 'remain' role set
            remain_set = {'maximum', 'yet', 'approximate', 'remained', 'remaining'}
            
            # Initialize a list to keep track of columns with the 'remain' role
            remain_cols = []
            
            # Iterate over the columns to check for the 'remain' role
            for col in df_col_roles.columns:
                # Get the set of words for the current column
                words_set = df_col_roles.at[0, col]
                
                # Calculate the intersection with the remain_set
                intersection = words_set.intersection(remain_set)
                
                # Check if the intersection is non-empty
                if intersection:
                    # Append the column to the remain_cols list
                    remain_cols.append(col)
            
            # If we found exactly one column with the 'remain' role, assign 'remain' to the second row
            if len(remain_cols) == 1:
                df_col_roles.at[1, remain_cols[0]] = 'remain'
            
    
            
            part_set = {'part', 'publicly'}
            
            # Initialize a list to keep track of columns with the 'part' role
            part_cols = []
            
            # Iterate over the columns to check for the 'part' role, excluding the 'remain' role column
            for col in df_col_roles.columns:
                if df_col_roles.at[1, col] == 'remain':
                    continue
                
                # Get the set of words for the current column
                words_set = df_col_roles.at[0, col]
                
                # Calculate the intersection with the part_set
                intersection = words_set.intersection(part_set)
                
                # Check if the intersection is non-empty
                if intersection:
                    # Append the column to the part_cols list
                    part_cols.append(col)
                    
            # If we found exactly one column with the 'part' role, assign 'part' to the second row
            if len(part_cols) == 1:
                df_col_roles.at[1, part_cols[0]] = 'part'
            
     
            # Check for the 'tot' role
            tot_cols = [col for col in df_col_roles.columns if pd.isna(df_col_roles.at[1, col])]
            
            # If there's exactly one column left without a role, assign 'tot' to it
            if len(tot_cols) == 1:
                df_col_roles.at[1, tot_cols[0]] = 'tot'

            try:
              
              cand_footnotes_in_text_after = extract_potential_footnotes(self.soup_after)
            
            except Exception as e:
                self.flow_dic['error_term_re']="extract_potential_footnotes"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)

            try:
              
               # Apply the function to all columns of the first row except the first column
               df.iloc[0, 1:] = df.iloc[0, 1:].apply(lambda x: out_paranth_footnote_into_paranth(x, cand_footnotes_in_text_after))
            
            except Exception as e:
                self.flow_dic['error_term_re']="out_paranth_footnote_into_paranth"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)

            try:
              
               
               df_footnotes = df.applymap(lambda x: table_footnote_extractor(x, cand_footnotes_in_text_after))
            
            except Exception as e:
                self.flow_dic['error_term_re']="table_footnote_extractor"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
            

            # Define the broken row indicators
            broken_indicators = ['to', 'To', 'Through', 'through', '-','–']
            
            # Iterate over the rows to identify and fix broken rows
            for i in range(len(df) - 1):
                if pd.isna(df.iloc[i,0]) or pd.isna(df.iloc[i+1,0]):
                    continue 
            
                # Check if the cell in column 0 ends with any of the broken_indicators and the rest of the row is all np.nan
                if any(df.iloc[i, 0].strip().endswith(ind) for ind in broken_indicators) and df.iloc[i, 1:].isna().all():
                    # Append the value of the current cell to the value of the next row's cell in column 0
                    df.iloc[i + 1, 0] = df.iloc[i, 0] + ' ' + df.iloc[i + 1, 0]
                    # Set the current cell to np.nan
                    df.iloc[i, 0] = np.nan
                    

            df = df.dropna(axis=0, how='all')
        
           
            are_all_rows_integers = all(isinstance(row, int) for row in df.index)
            if are_all_rows_integers:
                df.index = range(df.shape[0])
            
            try:
              
               
               df_lower=df.map(ends_text_strip)
               df_lower=df_lower.map(single_digit_or_letter_in_parenth_remover)
               
            
            except Exception as e:
                self.flow_dic['error_term_re']="ends_text_strip"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
            

            try:
              
               
               # Apply the function to each cell of the dataframe
               df_words2 = df_lower.map(convert_to_pattern_words)
            
            except Exception as e:
                self.flow_dic['error_term_re']="convert_to_pattern_words"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
            

            try:
              
               
               df_patterns=df_lower.map(convert_to_pattern) 
            
            except Exception as e:
                self.flow_dic['error_term_re']="convert_to_pattern_words"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
        

            interval_special=0
            patterns_list=list(df_patterns[0])
            
            
            pattern_dict = {}
            for i in range(len(patterns_list)):
                pattern=tuple(patterns_list[i])
                if pattern not in pattern_dict.keys():
                    pattern_dict[pattern]=[]
                pattern_dict[pattern].append(i)
                
            monthly_interval_rows = []
            monthly_interval_patterns=[]
            selected_monthly_interval_rows=[]
            selected_monthly_interval_pattern=[]
            
            for pattern, indices in pattern_dict.items():
               if len(indices) == 3 and all(df.iloc[idx, 0] for idx in indices): 
                    monthly_interval_rows.append(indices)
                    monthly_interval_patterns.append(pattern)
               
            if len(monthly_interval_rows) == 1:
                selected_monthly_interval_rows = monthly_interval_rows[0]
                selected_monthly_interval_pattern = monthly_interval_patterns[0]
                
                
            elif len(monthly_interval_rows) == 0:
                df_patterns_temp=df_lower.map(convert_to_pattern_without_comma) 
                patterns_list=list(df_patterns_temp[0])
                pattern_dict = {}
                for i in range(len(patterns_list)):
                    x=patterns_list[i]
                    y = x
                    
                    pattern=tuple(y)
                    if pattern not in pattern_dict:
                        pattern_dict[pattern]=[]
                    pattern_dict[pattern].append(i)
                
                for pattern, indices in pattern_dict.items():
                   if len(indices) == 3 and all(df.iloc[idx, 0] for idx in indices): 
                        monthly_interval_rows.append(indices)
                        monthly_interval_patterns.append(pattern)
                        
                if len(monthly_interval_rows) == 1:
                    
                   
                    selected_monthly_interval_rows = monthly_interval_rows[0]
                    selected_monthly_interval_pattern = monthly_interval_patterns[0]
                    df_patterns=df_lower.map(convert_to_pattern_without_comma) 
                    df_words2 = df_lower.map(convert_to_pattern_words_without_comma)
                    
                if len(monthly_interval_rows) == 0:
                   
                    patterns_list=list(df_patterns_temp[0])
                    pattern_dict = {}
                    for i in range(len(patterns_list)):
                        x=patterns_list[i]
                        y = [item for item in x if item!='num_label_y']
                        
                        pattern=tuple(y)
                        if pattern not in pattern_dict:
                            pattern_dict[pattern]=[]
                        pattern_dict[pattern].append(i)
                        
                    for pattern, indices in pattern_dict.items():
                       if len(indices) == 3 and all(df.iloc[idx, 0] for idx in indices): 
                            monthly_interval_rows.append(indices)
                            monthly_interval_patterns.append(pattern)
                            
                    if len(monthly_interval_rows) == 1:
                        selected_monthly_interval_rows = monthly_interval_rows[0]
                        selected_monthly_interval_pattern = monthly_interval_patterns[0]
                        df_patterns=df_lower.map(convert_to_pattern_without_comma) 
                        df_words2 = df_lower.map(convert_to_pattern_words_without_comma)
                        interval_special=1
                        
                
                
            else:
                def remove_num_month_elements(pattern):
                    return [elem for elem in pattern if elem not in {'month_name', 'num_label_y','num_label_dm','through','to'}]
                
                
                monthly_interval_patterns_num_month_dropped = [remove_num_month_elements(pattern) for pattern in monthly_interval_patterns]
                     
                
                def remove_short_elements(pattern_list):
                    return [elem for elem in pattern_list if len(elem) >= 2]
                
                # Apply the function to each list in monthly_interval_patterns_num_month_dropped
                monthly_interval_patterns_num_month_dropped_dropped = [
                    remove_short_elements(pattern) for pattern in monthly_interval_patterns_num_month_dropped
                ]
                
                # Find the index of the list with the smallest length
                index_of_smallest_list = min(range(len(monthly_interval_patterns_num_month_dropped_dropped)), key=lambda i: len(monthly_interval_patterns_num_month_dropped_dropped[i]))
                
                selected_monthly_interval_rows = monthly_interval_rows[index_of_smallest_list]
                selected_monthly_interval_pattern = monthly_interval_patterns[index_of_smallest_list]
                
            self.flow_dic['num_monthly_intervals']=  len(selected_monthly_interval_rows)  
            if len(selected_monthly_interval_rows)!=3:
                self.flow_dic['self_term_re']='not_3_monthly_intervals'
                return (self.flow_dic,self.df_output2)
           
            
            
            first_month=selected_monthly_interval_rows[0]
            second_month=selected_monthly_interval_rows[1]
            third_month=selected_monthly_interval_rows[2]
            
            tot_impos=0 
            tot_row=None
            
            if third_month==df.shape[0]-1:
                tot_impos=1 
            
            #first method:
            # Assume tot_impos is already defined and check if it's zero (indicating a total row is possible)
            if tot_impos == 0:
                # Start checking from the row immediately after the third_month
                start_index = third_month + 1
                # Initialize tot_row to None, in case no total row is found
                tot_row = None
            
                # Loop through each row starting from start_index to the end of the DataFrame
                for idx in range(start_index, df.shape[0]):
                    # Check if 'total' is a substring in the first column of df_lower at index idx
                    if pd.isna(df_lower.iloc[idx, 0]):
                        continue 
                    if 'total' in df_lower.iloc[idx, 0]:
                        tot_row = idx
                        break
            
        
            
            
            #second method:
            # Continue from the previous code where we search for 'total' using earlier methods
            if tot_row is None:
                # Check if there are non-NaN values in the third_month row, specifically in the columns beyond the first
                if not pd.isna(df.iloc[third_month, 1:]).all():
                    # Check if there is exactly one row after the third_month and that it is the last row of the DataFrame
                    if third_month + 1 == df.shape[0] - 1:
                        # This last row is identified as the total row
                        tot_row = third_month + 1
        
            
            
            #third method:
            # Assume tot_impos is already defined and check if it's zero (indicating a total row is possible)
            if tot_row is None:
                # Start checking from the row immediately after the third_month
                start_index = third_month + 1
               
            
                # Loop through each row starting from start_index to the end of the DataFrame
                for idx in range(start_index, df.shape[0]):
                    # Check if 'total' is a substring in the first column of df_lower at index idx
                    if pd.isna(df_lower.iloc[idx, 0]):
                        continue 
                    if 'quarter' in df_lower.iloc[idx, 0]:
                        tot_row = idx
                        break
            
        
            
            
            #fourth method:
            # Assume tot_impos is already defined and check if it's zero (indicating a total row is possible)
            if tot_row is None:
                # Start checking from the row immediately after the third_month
                start_index = third_month + 1
               
            
                # Loop through each row starting from start_index to the end of the DataFrame
                for idx in range(start_index, df.shape[0]):
                    # Check if 'total' is a substring in the first column of df_lower at index idx
                    if pd.isna(df_lower.iloc[idx, 0]):
                        continue 
                    if 'three' in df_lower.iloc[idx, 0]:
                        tot_row = idx
                        break
                    
                #fifth method:
                # Assume tot_impos is already defined and check if it's zero (indicating a total row is possible)
                if tot_row is None:
                    # Start checking from the row immediately after the third_month
                    for idx in range(third_month, df.shape[0]):
                        if not pd.isna(df.iloc[idx, 1:]).all():
                            start_index=idx+1
                            break
                    
                   
                   
                
                    # Loop through each row starting from start_index to the end of the DataFrame
                    for idx in range(start_index, df.shape[0]):
                        # Check if 'total' is a substring in the first column of df_lower at index idx
                        if pd.isna(df_lower.iloc[idx, 0]) and not pd.isna(df.iloc[idx, 1:]).all() and idx-start_index<=2:
                            tot_row = idx
                            break

        
            if tot_row is None:
                self.flow_dic['tot_row_found']=0
            else:
                self.flow_dic['tot_row_found']=1
            
           
            # Define the spans for the first, second, and third months.
            first_month_span = list(range(first_month, second_month))
            second_month_span = list(range(second_month, third_month))
            ali=second_month_span
            # Determine the 'almost_end_row'. If 'tot_row' is found, use it; otherwise, use the last row of the DataFrame.
            
            # there was a correction here
            almost_end_row = tot_row if tot_row is not None else df.shape[0] 
            third_month_span = list(range(third_month, almost_end_row))
            
        
           
            
            
            # Initialize an empty dictionary to store the labels and their corresponding month occurrences.
            monthly_interval_labels = {}
            
            # Helper function to populate the dictionary
            def update_labels(span, month_number):
                for idx in span:
                    label = df.iloc[idx, 0]  # Assuming the label is in the first column
                    if label not in monthly_interval_labels:
                        monthly_interval_labels[label] = [month_number]
                    elif month_number not in monthly_interval_labels[label]:
                        monthly_interval_labels[label].append(month_number)  
            
            # Update the dictionary with labels from each month span
            update_labels(first_month_span[1:], 1)  # Exclude the first row of the span as it is the title row
            update_labels(second_month_span[1:], 2)  # Exclude the first row of the span
            update_labels(third_month_span[1:], 3)  # Exclude the first row of the span
            
        
            
            
            # Initialize tot_span with tot_row, if it exists
            if tot_row is not None:
                tot_span = [tot_row]
            
                # Check if tot_row is not the last row in the DataFrame
                if tot_row < df.shape[0] - 1:
                    # Iterate through the rows following tot_row to check for known labels
                    for idx in range(tot_row + 1, df.shape[0]):
                        label = df.iloc[idx, 0]  # Assuming the label is in the first column
                        if label in monthly_interval_labels:
                            tot_span.append(idx)
                        else:
                            break  # Stop if a row does not contain a recognized label
            else:
                tot_span = []
            
        
            
            
            # Define end_row based on the existence and contents of tot_row and tot_span
            if tot_row is not None and tot_span:
                end_row = tot_span[-1]  # Last element of tot_span
            else:
                end_row = third_month_span[-1]  # Last row of the third_month_span if tot_row is not defined
            
        
            
            
            # now let's focus on how to cut rows and clean up our monhtly_interval + total row. 
            df_cut2 = df.iloc[:end_row + 1]
            
            # Initialize the new column in df_cut2 with np.nan
            df_cut2['id'] = np.nan
            
            # Helper function to update the new column based on given span and label
            def assign_label_to_span(span, label):
                df_cut2.loc[span, 'id'] = label
            
            # Assign labels to spans
            assign_label_to_span(first_month_span, 1)
            assign_label_to_span(second_month_span, 2)
            assign_label_to_span(third_month_span, 3)
            
            # Check if tot_span is defined and not empty
            if tot_span:
                assign_label_to_span(tot_span, 4)
            
            #now let's make a dictionary of the monthly_interval dates.
            
            monthly_interval_dates={}
            
            monthly_interval_dates[1]=df_words2[0].iloc[first_month]
            monthly_interval_dates[2]=df_words2[0].iloc[second_month]
            monthly_interval_dates[3]=df_words2[0].iloc[third_month]
            
            
            monthly_interval_patterns={}
            
            monthly_interval_patterns[1]=df_patterns[0].iloc[first_month]
            monthly_interval_patterns[2]=df_patterns[0].iloc[second_month]
            monthly_interval_patterns[3]=df_patterns[0].iloc[third_month]
            
            
            monthly_interval_dates_converted={}
            
            monthly_list=[1,2,3]
            
            if interval_special==0:
                pattern_interval=list(monthly_interval_patterns.values())[0]
                single_date_pattern=1 
                
                pattern_interval_processed=filter_specific_labels(pattern_interval)
                
                bias=0
                counter=0
                

                
                while(counter<2):
                    
                    if pattern_interval_processed==['month_name', 'num_label_dm', 'num_label_y','month_name', 'num_label_dm', 'num_label_y']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                            if len(whole_date_processed)%2!=0:
                                print("sinlge_date_pattern is not making sense")
                                break 
                            
                            slicer=int(len(whole_date_processed)/2)
                            
                            first_processed=whole_date_processed[:slicer]
                            second_processed=whole_date_processed[slicer:]
                            
                            first_day=int(first_processed[1])
                            second_day=int(second_processed[1])
                            
                            first_month=month_to_number(first_processed[0])
                            second_month=month_to_number(second_processed[0])
                            
                            first_year=int(first_processed[2])
                            second_year=int(second_processed[2])
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)
                            
                    if pattern_interval_processed==['month_name', 'num_label_dm', 'num_label_dm', 'num_label_y']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                    
                            
                    
                            
                            first_day=int(whole_date_processed[1])
                            second_day=int(whole_date_processed[2])
                            
                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[0])
                            
                            first_year=int(whole_date_processed[3])
                            second_year=int(whole_date_processed[3])
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)
                    
                    if pattern_interval_processed==['month_name']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                            
                    
                            
                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[0])
                            
                            first_year=self.period_year
                            second_year=self.period_year
                            
                            first_day=1
                            second_day=days_in_month(second_year,second_month)
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)
                            
                            
                                                
                    if pattern_interval_processed==['month_name', 'num_label_dm', 'month_name', 'num_label_dm']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                            
                    
                            
                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[2])
                            
                            
                            first_day=int(whole_date_processed[1])
                            second_day=int(whole_date_processed[3])
                            
                            first_year=self.period_year
                            second_year=self.period_year
                            

                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)
                    
                    
                    
                    if pattern_interval_processed==['month_name', 'num_label_y']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                    
                            
                            
                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[0])
                            
                            first_year=int(whole_date_processed[1])
                            second_year=int(whole_date_processed[1])
                            
                            first_day=1
                            second_day=days_in_month(second_year,second_month)
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)
                            
                    if pattern_interval_processed==['month_name', 'num_label_dm', 'num_label_y']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                    
                
                            
                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[0])
                            
                            first_year=int(whole_date_processed[2])
                            second_year=int(whole_date_processed[2])
                            
                            first_day=1
                            second_day=int(whole_date_processed[1])
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)                        
                            
                    if pattern_interval_processed== ['month_name', 'num_label_dm', 'num_label_dm']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                    
                            
                    
                            
                    
                            
                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[0])
                            
                            first_year=self.period_year
                            second_year==self.period_year
                            
                            first_day=int(whole_date_processed[1])
                            second_day=int(whole_date_processed[2])
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)
                            
                            
                    
                    if pattern_interval_processed== ['num_label_dm','num_label_dm','num_label_dm','num_label_dm','num_label_dm','num_label_dm']:
                        first_numbers=[]
                        second_numbers=[]
                        for i in monthly_list:
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                            
                            first_numbers.append(int(whole_date_processed[0]))
                            first_numbers.append(int(whole_date_processed[3+0]))
                            
                            second_numbers.append(int(whole_date_processed[1]))
                            second_numbers.append(int(whole_date_processed[3+1]))
                            
                        max_first_numbers=max(first_numbers)
                        max_second_numbers=max(second_numbers)
                        
                        if max_first_numbers<=12 and max_second_numbers>12:
                            for i in monthly_list:
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                
                                first_month=int(whole_date_processed[0])
                                second_month=int(whole_date_processed[0+3])
                                
                                first_day=int(whole_date_processed[1])
                                second_day=int(whole_date_processed[1+3])
                                
                                first_year=2000+int(whole_date_processed[2])
                                second_year=2000+int(whole_date_processed[2+3])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                                
                                
                        if max_first_numbers>12 and max_second_numbers<=12:
                            for i in monthly_list:
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                
                                first_month=int(whole_date_processed[1])
                                second_month=int(whole_date_processed[1+3])
                                
                                first_day=int(whole_date_processed[0])
                                second_day=int(whole_date_processed[0+3])
                                
                                first_year=2000+int(whole_date_processed[2])
                                second_year=2000+int(whole_date_processed[2+3])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                                
                            
                                
                    if pattern_interval_processed== ['month_name', 'num_label_dm', 'month_name', 'num_label_dm', 'num_label_y']:
                        for i in monthly_list:
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]

                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[2])
                            
                            first_year=int(whole_date_processed[4])
                            second_year=int(whole_date_processed[4])
                            
                            first_day=int(whole_date_processed[1])
                            second_day=int(whole_date_processed[3])
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)
                                
                                
                                
                    if pattern_interval_processed== ['num_label_dm','num_label_dm','num_label_y','num_label_dm','num_label_dm','num_label_y']:
                        first_numbers=[]
                        second_numbers=[]
                        for i in monthly_list:
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                            
                            first_numbers.append(int(whole_date_processed[0]))
                            first_numbers.append(int(whole_date_processed[3+0]))
                            
                            second_numbers.append(int(whole_date_processed[1]))
                            second_numbers.append(int(whole_date_processed[3+1]))
                            
                        max_first_numbers=max(first_numbers)
                        max_second_numbers=max(second_numbers)
                        
                        if max_first_numbers<=12 and max_second_numbers>12:
                            for i in monthly_list:
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                
                                first_month=int(whole_date_processed[0])
                                second_month=int(whole_date_processed[0+3])
                                
                                first_day=int(whole_date_processed[1])
                                second_day=int(whole_date_processed[1+3])
                                
                                first_year=int(whole_date_processed[2])
                                second_year=int(whole_date_processed[2+3])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                                
                                
                        if max_first_numbers>12 and max_second_numbers<=12:
                            for i in monthly_list:
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                
                                first_month=int(whole_date_processed[1])
                                second_month=int(whole_date_processed[1+3])
                                
                                first_day=int(whole_date_processed[0])
                                second_day=int(whole_date_processed[0+3])
                                
                                first_year=int(whole_date_processed[2])
                                second_year=int(whole_date_processed[2+3])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                    
                    if len(monthly_interval_dates_converted)>0:    
                        break
                        
                    
                    #  especial cases     
                    if len(monthly_interval_dates_converted)==0:
                        if 'period' in pattern_interval or 'month' in pattern_interval:
                            if pattern_interval_processed[0]=='num_label_dm':
                                pattern_interval_processed=pattern_interval_processed[1:]
                                bias=1
                                counter+=1
                                continue
                               
                    if len(monthly_interval_dates_converted)==0:
                        if '#' in pattern_interval:
                            if pattern_interval_processed[0]=='num_label_dm':
                                pattern_interval_processed=pattern_interval_processed[1:]
                                bias=1
                                counter+=1
                                continue
                            
                    counter+=1        
                            
                            
                            
                if len(monthly_interval_dates_converted)!=3:
                    self.flow_dic['self_term_re']='monthly_interval_dates_converted_issue'
                    return (self.flow_dic,self.df_output2)
        
        
         
            if interval_special==1:
                for i in monthly_list:
                    
                    pattern_interval=monthly_interval_patterns[i]
                    single_date_pattern=1 
                    
                    pattern_interval_processed=filter_specific_labels(pattern_interval)
                    
                    bias=0
                    counter=0
                    
    
                    
                    while(counter<2):
                        
                        
                        
                        if pattern_interval_processed==['month_name', 'num_label_dm', 'num_label_y','month_name', 'num_label_dm', 'num_label_y']:
                            
                                
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                if len(whole_date_processed)%2!=0:
                                    print("sinlge_date_pattern is not making sense")
                                    break 
                                
                                slicer=int(len(whole_date_processed)/2)
                                
                                first_processed=whole_date_processed[:slicer]
                                second_processed=whole_date_processed[slicer:]
                                
                                first_day=int(first_processed[1])
                                second_day=int(second_processed[1])
                                
                                first_month=month_to_number(first_processed[0])
                                second_month=month_to_number(second_processed[0])
                                
                                first_year=int(first_processed[2])
                                second_year=int(second_processed[2])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                                
                        if pattern_interval_processed==['month_name', 'num_label_dm', 'num_label_dm', 'num_label_y']:
                           
                                
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                        
         
                                first_day=int(whole_date_processed[1])
                                second_day=int(whole_date_processed[2])
                                
                                first_month=month_to_number(whole_date_processed[0])
                                second_month=month_to_number(whole_date_processed[0])
                                
                                first_year=int(whole_date_processed[3])
                                second_year=int(whole_date_processed[3])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                        
                        if pattern_interval_processed==['month_name']:
                            
                                
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                
                        
                                
                                first_month=month_to_number(whole_date_processed[0])
                                second_month=month_to_number(whole_date_processed[0])
                                
                                first_year=self.period_year
                                second_year=self.period_year
                                
                                first_day=1
                                second_day=days_in_month(second_year,second_month)
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                                
                                
                                                    
                        if pattern_interval_processed==['month_name', 'num_label_dm', 'month_name', 'num_label_dm']:
                           
                                
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                
                        
                                
                                first_month=month_to_number(whole_date_processed[0])
                                second_month=month_to_number(whole_date_processed[2])
                                
                                
                                first_day=int(whole_date_processed[1])
                                second_day=int(whole_date_processed[3])
                                
                                first_year=self.period_year
                                second_year=self.period_year
                                
    
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                        
                        
                        
                        if pattern_interval_processed==['month_name', 'num_label_y']:
                           
                                
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                        
      
                                
                                first_month=month_to_number(whole_date_processed[0])
                                second_month=month_to_number(whole_date_processed[0])
                                
                                first_year=int(whole_date_processed[1])
                                second_year=int(whole_date_processed[1])
                                
                                first_day=1
                                second_day=days_in_month(second_year,second_month)
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                                
                        if pattern_interval_processed==['month_name', 'num_label_dm', 'num_label_y']:
                        
                            
                            whole_date=monthly_interval_dates[i]
                            whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                            whole_date_processed=whole_date_processed[bias:]
                    

                            first_month=month_to_number(whole_date_processed[0])
                            second_month=month_to_number(whole_date_processed[0])
                            
                            first_year=int(whole_date_processed[2])
                            second_year=int(whole_date_processed[2])
                            
                            first_day=1
                            second_day=int(whole_date_processed[1])
                            
                            first_date = datetime.date(first_year, first_month, first_day)
                            second_date = datetime.date(second_year, second_month, second_day)
                            
                            monthly_interval_dates_converted[i]=(first_date,second_date)    
                        if pattern_interval_processed== ['month_name', 'num_label_dm', 'num_label_dm']:
                           
                                
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                 
                                first_month=month_to_number(whole_date_processed[0])
                                second_month=month_to_number(whole_date_processed[0])
                                
                                first_year=self.period_year
                                second_year==self.period_year
                                
                                first_day=int(whole_date_processed[1])
                                second_day=int(whole_date_processed[2])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                        

                        if pattern_interval_processed== ['month_name', 'num_label_dm', 'month_name', 'num_label_dm', 'num_label_y']:
                            
                                
                                whole_date=monthly_interval_dates[i]
                                whole_date_processed=filter_specific_words(whole_date,pattern_interval)
                                whole_date_processed=whole_date_processed[bias:]
                                
            
                                first_month=month_to_number(whole_date_processed[0])
                                second_month=month_to_number(whole_date_processed[2])
                                
                                first_year=int(whole_date_processed[4])
                                second_year=int(whole_date_processed[4])
                                
                                first_day=int(whole_date_processed[1])
                                second_day=int(whole_date_processed[3])
                                
                                first_date = datetime.date(first_year, first_month, first_day)
                                second_date = datetime.date(second_year, second_month, second_day)
                                
                                monthly_interval_dates_converted[i]=(first_date,second_date)
                                    
                                    

                        
                        if i in monthly_interval_dates_converted.keys():  
                            
                            break
                            
                        
                        #  especial cases     
                        if i not in monthly_interval_dates_converted.keys():   
                            if 'period' in pattern_interval or 'month' in pattern_interval:
                                if pattern_interval_processed[0]=='num_label_dm':
                                    pattern_interval_processed=pattern_interval_processed[1:]
                                    bias=1
                                    counter+=1
                                    continue
                                   
                        if i not in monthly_interval_dates_converted.keys(): 
                            if '#' in pattern_interval:
                                if pattern_interval_processed[0]=='num_label_dm':
                                    pattern_interval_processed=pattern_interval_processed[1:]
                                    bias=1
                                    counter+=1
                                    continue
                                
                        counter+=1        
                                
                                
                                
                if len(monthly_interval_dates_converted)!=3:
                    self.flow_dic['self_term_re']='monthly_interval_dates_converted_issue'
                    return (self.flow_dic,self.df_output2)
            
        
             
                    
                    
                
            
            # Now, df_cut2 has an additional column where
            # 1, 2, 3 represent the first, second, and third month spans, respectively,
            # 4 represents the total span, and other rows remain np.nan indicating they belong to none of these spans.
            
            # Initialize the 'rank' column with a default value
            df_cut2['rank'] = 0
            
            # Use groupby on the 'id' column and apply cumcount to enumerate rows within each group
            df_cut2['rank'] = df_cut2.groupby('id').cumcount()
            
            
            
                
            
            # Find the actual row index for id=1 and rank=0
            first_month_index = df_cut2[(df_cut2['id'] == 1) & (df_cut2['rank'] == 0)].index.min()
            
            if first_month_index ==2:
                # Check the row above the actual first_month row
                row_above_index = first_month_index - 1
            
                # Check all values in the row above
                if row_above_index > 0:  # Ensure it's not the header row
                    row_above = df_cut2.iloc[row_above_index]
                    if all(check_single_digit_or_letter(value) for value in row_above):
                        # Merge row_above into the header
                        header_row = df_cut2.iloc[0]
                        new_header = header_row.combine_first(row_above)
                        
                        # Update the DataFrame
                        df_cut2.iloc[0] = new_header
                        df_cut2.drop(row_above_index, inplace=True)
                        df_cut2.reset_index(drop=True, inplace=True)  # Reset index after dropping a row
            
            first_month_index = df_cut2[(df_cut2['id'] == 1) & (df_cut2['rank'] == 0)].index.min()
            
            
            if first_month_index > 1:
                # Check rows from index 1 to first_month_index-1
                for index in range(1, first_month_index):
                    row = df_cut2.iloc[index]
                    
                    # Check if all columns except the first are np.nan
                    if row.iloc[1:].isna().all():
                        
                        df_cut2.drop(index, inplace=True)
            
            
            df_cut2.reset_index(drop=True, inplace=True)
            
            # Assume df_cut2 has been defined and modified as per previous steps
            first_month_index = df_cut2[(df_cut2['id'] == 1) & (df_cut2['rank'] == 0)].index.min()
            
            # Recheck if the first month index is still not 1
            if first_month_index > 1:
                # Cut the rows from index 1 to first_month_index-1 (exclusive of the row at first_month_index)
                df_mid_left_overs = df_cut2.iloc[1:first_month_index].copy()
            
                # Drop these rows from df_cut2
                df_cut2.drop(df_cut2.index[1:first_month_index], inplace=True)
                
                # Reset index of df_cut2 to ensure it starts at 0 again
                df_cut2.reset_index(drop=True, inplace=True)
            
            
            
            
            
            # Assuming df_cut2 has been previously defined and contains 'id' and 'rank' columns
            
            # Iterate over each unique ID in df_cut2
            for id_value in df_cut2['id'].unique():
                # Filter rows for the current ID
                group = df_cut2[df_cut2['id'] == id_value]
                
                # Drop the unneeded columns just for checking emptiness
                # Adjust column indices accordingly if column 0 is not literally the first column
                check_columns = group.drop(columns=[group.columns[0], 'id', 'rank'])
                
                # Find indices where all selected columns are empty
                empty_indices = check_columns[check_columns.isna().all(axis=1)].index
                
                # Ensure that we do not drop all rows by checking if the group size is greater than the number of empty rows
                if len(group) > len(empty_indices):
                    # Remove empty rows, preserving at least one row
                    # To keep at least one row, exclude the first row index from the droppable indices if all rows are empty
                    if len(empty_indices) == len(group):
                        empty_indices = empty_indices[1:]
                    
                    df_cut2.drop(index=empty_indices, inplace=True)
            
            # Reset index after dropping rows
            df_cut2.reset_index(drop=True, inplace=True)
            
                        
            # Create df_cut from the original DataFrame df by slicing from the first row to end_row
            df_cut = df.iloc[:end_row + 1]  # end_row + 1 to include end_row in the slice
            

            df_cut_lower=df_cut.map(ends_text_strip)
                
            df_cut_lower_unit=df_cut_lower.map( unit_extractor)
            
            
            df_cut_lower_unit_translated=df_cut_lower_unit.map(unit_analyser)

            df_dollar=df_cut.map(dollar_extractor)

            df_identify = pd.DataFrame(np.nan, index=[0, 1, 2], columns=df.columns)
   
            # Assuming df_col_roles already exists and has the roles in its second row
            
            # Fill the first row of df_identify
            df_identify.iloc[0, 0] = 'period'
            df_identify.iloc[0, 1:] = df_col_roles.iloc[1, :]
            
            df_identify.iloc[1, :] = df_dollar.max(axis=0)
            

            # Explicitly set the data type for the 'values' column to 'object' to handle list entries
            df_unit_source_stat = pd.DataFrame({
                'source_dum': [0, 0, 0, 0],
                'values': [np.nan] * 4,  # Initialize with NaNs
                'values_type': [np.nan] * 4,
                'all_same_type':np.nan,
                'unique_type':np.nan,
                'y_in_it': np.nan,
                'error': [[] for _ in range(4)] 
                
            }, index=['table', 'above', 'top', 'after']).astype({'values': 'object','values_type':'object'})  # Set 'values' as object type
            
            df_unit_source_stat.index.name = 'source'

   
            # Initialize controlling variables
            unit_in_table_exist = 0
            unique_unit_type_in_table_dum = None
            unique_unit_type_in_table = None

            potential_source_of_unit={}
            unit_overwrite=0
            float_unit_contradiction=0
            
            too_many_str_units=0
            y_in_str_units=0
            
            # Get all the non-NaN values from df_cut_lower_unit_translated in a list
            non_nan_values = df_cut_lower_unit_translated.stack().dropna().tolist()
            
            # Check if there are any non-NaN values
            if len(non_nan_values) > 0:
                unit_in_table_exist = 1
                
                df_unit_source_stat.loc['table', 'source_dum']=1
                df_unit_source_stat.loc['table', 'values']=list(set(non_nan_values))
            
                potential_source_of_unit['table']=set(non_nan_values)
            
                # Determine the unique types in non_nan_values
                types_in_table = set(type(value) for value in non_nan_values)
                
                numerical_types_in_table = [0 if t == float else 1 if t == str else -1 for t in types_in_table]
            
                
                df_unit_source_stat.loc['table', 'values_type']=numerical_types_in_table
                # Check if there is only one unique type
                
                if len(types_in_table) > 1:
                    df_unit_source_stat.loc['table', 'error'].append('multiple_types')
                if -1 in numerical_types_in_table:
                    df_unit_source_stat.loc['table', 'error'].append('unknown_types')
                    
                if len(types_in_table) == 1:
                    unique_unit_type_in_table_dum = 1
                    df_unit_source_stat.at['table', 'all_same_type']=1
                    # Determine the unique type
                    unique_type = next(iter(types_in_table))
                    if unique_type == float:
                        unique_unit_type_in_table = 0
                        df_unit_source_stat.at['table', 'unique_type']=0
                    elif unique_type == str:
                        unique_unit_type_in_table = 1
                        df_unit_source_stat.at['table', 'unique_type']=1
            
            
            
            
            if unit_in_table_exist == 1 and unique_unit_type_in_table_dum == 1 and unique_unit_type_in_table == 0:
                # Iterate over each column in df_cut_lower_unit_translated
                for column in df_cut_lower_unit_translated.columns:
                    # Get the non-NaN values in the current column
                    non_nan_values_in_column = df_cut_lower_unit_translated[column].dropna().unique()
                    
                    # Check if there are any non-NaN values
                    if len(non_nan_values_in_column) == 0:
                        # If no non-NaN values, set the corresponding value in df_identify to np.nan
                        df_identify.loc[2, column] = np.nan
                    elif len(non_nan_values_in_column) == 1:
                        # If there is exactly one unique non-NaN value, set it in the corresponding column of row 2 in df_identify
                        current_value = df_identify.loc[2, column]
                        value=non_nan_values_in_column[0]
                        if not pd.isna(current_value) and current_value != value:
                            df_unit_source_stat.loc['table', 'error'].append('unit_overwrite')
                            unit_overwrite=1
                        df_identify.loc[2, column] = value
                    else:
                        float_unit_contradiction=1
                        df_unit_source_stat.loc['table', 'error'].append('float_contradiction')
                       
            
            if unit_in_table_exist == 1 and unique_unit_type_in_table_dum == 1 and unique_unit_type_in_table == 1:
                # Get all unique string values from non_nan_values
                unique_strings = set(non_nan_values)
                
                # Check the number of unique string values
                if len(unique_strings) > 2:
                    df_unit_source_stat.loc['table', 'error'].append('too_many_str_units')
                    too_many_str_units=1
                else:
                    # Check if any of the unique strings contain the letter 'y'
                    if any('y' in s for s in unique_strings):
                        y_in_str_units=1
                        df_unit_source_stat.loc['table', 'error'].append('y_in_str_units')
                    else:
                        # Loop over values in unique_strings
                        for value in unique_strings:
                            if value in ['a1', 'a2', 'a3']:
                                # Extract the digit from the value
                                digit = int(value[1])
                                # Update the second row of df_identify for all columns except 'price' or 'period'
                                for column in df_identify.columns:
                                    if df_identify.loc[0, column] not in ['price', 'period']:
                                        current_value = df_identify.loc[2, column]
                                        if not pd.isna(current_value) and current_value != digit:
                                            df_unit_source_stat.loc['table', 'error'].append('unit_overwrite')
                                            unit_overwrite=1
                                        df_identify.loc[2, column] = digit
                            elif value in ['s1', 's2', 's3', 'd1', 'd2', 'd3']:
                                # Extract the prefix and the digit from the value
                                prefix = value[0]
                                digit = int(value[1])
                                # Update the second row of df_identify based on prefix and digit
                                for column in df_identify.columns:
                                    if df_identify.loc[0, column] not in ['price', 'period']:
                                        if prefix == 'd' and df_identify.loc[1, column] == 1:
                                            # Dollar column
                                            current_value = df_identify.loc[2, column]
                                            if not pd.isna(current_value) and current_value != digit:
                                                df_unit_source_stat.loc['table', 'error'].append('unit_overwrite')
                                                unit_overwrite=1
                                            df_identify.loc[2, column] = digit
                                        elif prefix == 's' and df_identify.loc[1, column] == 0:
                                            # Share column
                                            current_value = df_identify.loc[2, column]
                                            if not pd.isna(current_value) and current_value != digit:
                                                df_unit_source_stat.loc['table', 'error'].append('unit_overwrite')
                                                unit_overwrite=1
                                            df_identify.loc[2, column] = digit
                            elif len(value) == 4:
                                # Split the value into two 2-character strings
                                part1, part2 = value[:2], value[2:]
                                for part in [part1, part2]:
                                    if part in ['a1', 'a2', 'a3']:
                                        digit = int(part[1])
                                        for column in df_identify.columns:
                                            if df_identify.loc[0, column] not in ['price', 'period']:
                                                current_value = df_identify.loc[2, column]
                                                if not pd.isna(current_value) and current_value != digit:
                                                    df_unit_source_stat.loc['table', 'error'].append('unit_overwrite')
                                                    unit_overwrite=1
                                                df_identify.loc[2, column] = digit
                                    elif part in ['s1', 's2', 's3']:
                                        digit = int(part[1])
                                        for column in df_identify.columns:
                                            if df_identify.loc[0, column] not in ['price', 'period']:
                                                if df_identify.loc[1, column] == 0:
                                                    current_value = df_identify.loc[2, column]
                                                    if not pd.isna(current_value) and current_value != digit:
                                                        df_unit_source_stat.loc['table', 'error'].append('unit_overwrite')
                                                        unit_overwrite=1
                                                    df_identify.loc[2, column] = digit
                                    elif part in ['d1', 'd2', 'd3']:
                                        digit = int(part[1])
                                        for column in df_identify.columns:
                                            if df_identify.loc[0, column] not in ['price', 'period']:
                                                if df_identify.loc[1, column] == 1:
                                                    current_value = df_identify.loc[2, column]
                                                    if not pd.isna(current_value) and current_value != digit:
                                                        df_unit_source_stat.loc['table', 'error'].append('unit_overwrite')
                                                        unit_overwrite=1
                                                    df_identify.loc[2, column] = digit
            
            

            y_in_str_above_units=0
            above_unit_overwrite=0
            
            unit_in_text=None
            text_before_table_cleaned=self.soup_before.get_text(separator=' ', strip=True)
            unit_in_text= unit_extracted_for_text(text_before_table_cleaned)
            
            if unit_in_text:
                unit_in_text_analysed=unit_analyser(unit_in_text)
                df_unit_source_stat.loc['above', 'source_dum']=1
                potential_source_of_unit['above']=[unit_in_text_analysed]
                df_unit_source_stat.at['above', 'values']=[unit_in_text_analysed]
                unique_strings=[unit_in_text_analysed]
                
                if type(unit_in_text_analysed)==float:
                    df_unit_source_stat.loc['above', 'error'].append('float_type')
                
                if any('y' in s for s in unique_strings):
                   df_unit_source_stat.loc['above', 'error'].append('y_in_str_units')
                   y_in_str_above_units=1
                else:
                    # Loop over values in unique_strings
                    for value in unique_strings:
                        if value in ['a1', 'a2', 'a3']:
                            # Extract the digit from the value
                            digit = int(value[1])
                            # Update the second row of df_identify for all columns except 'price' or 'period'
                            for column in df_identify.columns:
                                if df_identify.loc[0, column] not in ['price', 'period']:
                                    current_value = df_identify.loc[2, column]
                                    if not pd.isna(current_value) and current_value != digit:
                                        df_unit_source_stat.loc['above', 'error'].append('unit_overwrite')
                                        above_unit_overwrite=1
                                    df_identify.loc[2, column] = digit
                        elif value in ['s1', 's2', 's3', 'd1', 'd2', 'd3']:
                            # Extract the prefix and the digit from the value
                            prefix = value[0]
                            digit = int(value[1])
                            # Update the second row of df_identify based on prefix and digit
                            for column in df_identify.columns:
                                if df_identify.loc[0, column] not in ['price', 'period']:
                                    if prefix == 'd' and df_identify.loc[1, column] == 1:
                                        # Dollar column
                                        current_value = df_identify.loc[2, column]
                                        if not pd.isna(current_value) and current_value != digit:
                                           df_unit_source_stat.loc['above', 'error'].append('unit_overwrite')
                                           above_unit_overwrite=1
                                        df_identify.loc[2, column] = digit
                                    elif prefix == 's' and df_identify.loc[1, column] == 0:
                                        # Share column
                                        current_value = df_identify.loc[2, column]
                                        if not pd.isna(current_value) and current_value != digit:
                                            df_unit_source_stat.loc['above', 'error'].append('unit_overwrite')
                                            above_unit_overwrite=1
                                        df_identify.loc[2, column] = digit
                        elif len(value) == 4:
                            # Split the value into two 2-character strings
                            part1, part2 = value[:2], value[2:]
                            for part in [part1, part2]:
                                if part in ['a1', 'a2', 'a3']:
                                    digit = int(part[1])
                                    for column in df_identify.columns:
                                        if df_identify.loc[0, column] not in ['price', 'period']:
                                            current_value = df_identify.loc[2, column]
                                            if not pd.isna(current_value) and current_value != digit:
                                                df_unit_source_stat.loc['above', 'error'].append('unit_overwrite')
                                                above_unit_overwrite=1
                                            df_identify.loc[2, column] = digit
                                elif part in ['s1', 's2', 's3']:
                                    digit = int(part[1])
                                    for column in df_identify.columns:
                                        if df_identify.loc[0, column] not in ['price', 'period']:
                                            if df_identify.loc[1, column] == 0:
                                                current_value = df_identify.loc[2, column]
                                                if not pd.isna(current_value) and current_value != digit:
                                                    df_unit_source_stat.loc['above', 'error'].append('unit_overwrite')
                                                    above_unit_overwrite=1
                                                df_identify.loc[2, column] = digit
                                elif part in ['d1', 'd2', 'd3']:
                                    digit = int(part[1])
                                    for column in df_identify.columns:
                                        if df_identify.loc[0, column] not in ['price', 'period']:
                                            if df_identify.loc[1, column] == 1:
                                                current_value = df_identify.loc[2, column]
                                                if not pd.isna(current_value) and current_value != digit:
                                                    df_unit_source_stat.loc['above', 'error'].append('unit_overwrite')
                                                    above_unit_overwrite=1
                                                df_identify.loc[2, column] = digit
            
            
            
                
            units_in_after_contents= extract_units_from_after_contents(self.soup_after)
            
            too_many_str_after_units=0
            y_in_str_after_units=0
            after_unit_overwrite=0
            
            if units_in_after_contents:
                df_unit_source_stat.loc['after', 'source_dum']=1
                units_in_after_values=list(units_in_after_contents.values())
                units_in_after_analysed=[unit_analyser(x) for x in units_in_after_values ]
                
                potential_source_of_unit['after']=units_in_after_analysed
                df_unit_source_stat.at['after', 'values']=units_in_after_analysed
                units_in_after_analysed_type = [0 if type(t) == float else 1 if type(t) == str else -1 for t in units_in_after_analysed]
                
                if len(set(units_in_after_analysed_type)) > 1:
                    df_unit_source_stat.loc['after', 'error'].append('multiple_types')
                if -1 in units_in_after_analysed_type:
                    df_unit_source_stat.loc['after', 'error'].append('unknown_types')
                if 0 in units_in_after_analysed_type:
                    df_unit_source_stat.loc['after', 'error'].append('float_type')
                    
                
                
                if len(units_in_after_analysed)>2:
                    df_unit_source_stat.loc['after', 'error'].append('too_many_str_units')
                    too_many_str_after_units=1
                    
                if len(units_in_after_analysed)<=2:
                    unique_strings=units_in_after_analysed
                    
                    if any('y' in s for s in unique_strings):
                        df_unit_source_stat.loc['after', 'error'].append('y_in_str_units')
                        y_in_str_after_units=1
                    else:
                        # Loop over values in unique_strings
                        for value in unique_strings:
                            if value in ['a1', 'a2', 'a3']:
                                # Extract the digit from the value
                                digit = int(value[1])
                                # Update the second row of df_identify for all columns except 'price' or 'period'
                                for column in df_identify.columns:
                                    if df_identify.loc[0, column] not in ['price', 'period']:
                                        current_value = df_identify.loc[2, column]
                                        if not pd.isna(current_value) and current_value != digit:
                                            df_unit_source_stat.loc['after', 'error'].append('unit_overwrite')
                                            after_unit_overwrite=1
                                        df_identify.loc[2, column] = digit
                            elif value in ['s1', 's2', 's3', 'd1', 'd2', 'd3']:
                                # Extract the prefix and the digit from the value
                                prefix = value[0]
                                digit = int(value[1])
                                # Update the second row of df_identify based on prefix and digit
                                for column in df_identify.columns:
                                    if df_identify.loc[0, column] not in ['price', 'period']:
                                        if prefix == 'd' and df_identify.loc[1, column] == 1:
                                            # Dollar column
                                            current_value = df_identify.loc[2, column]
                                            if not pd.isna(current_value) and current_value != digit:
                                                df_unit_source_stat.loc['after', 'error'].append('unit_overwrite')
                                                after_unit_overwrite=1
                                            df_identify.loc[2, column] = digit
                                        elif prefix == 's' and df_identify.loc[1, column] == 0:
                                            # Share column
                                            current_value = df_identify.loc[2, column]
                                            if not pd.isna(current_value) and current_value != digit:
                                                df_unit_source_stat.loc['after', 'error'].append('unit_overwrite')
                                                after_unit_overwrite=1
                                            df_identify.loc[2, column] = digit
                            elif len(value) == 4:
                                # Split the value into two 2-character strings
                                part1, part2 = value[:2], value[2:]
                                for part in [part1, part2]:
                                    if part in ['a1', 'a2', 'a3']:
                                        digit = int(part[1])
                                        for column in df_identify.columns:
                                            if df_identify.loc[0, column] not in ['price', 'period']:
                                                current_value = df_identify.loc[2, column]
                                                if not pd.isna(current_value) and current_value != digit:
                                                    df_unit_source_stat.loc['after', 'error'].append('unit_overwrite')
                                                    after_unit_overwrite=1
                                                df_identify.loc[2, column] = digit
                                    elif part in ['s1', 's2', 's3']:
                                        digit = int(part[1])
                                        for column in df_identify.columns:
                                            if df_identify.loc[0, column] not in ['price', 'period']:
                                                if df_identify.loc[1, column] == 0:
                                                    current_value = df_identify.loc[2, column]
                                                    if not pd.isna(current_value) and current_value != digit:
                                                        df_unit_source_stat.loc['after', 'error'].append('unit_overwrite')
                                                        after_unit_overwrite=1
                                                    df_identify.loc[2, column] = digit
                                    elif part in ['d1', 'd2', 'd3']:
                                        digit = int(part[1])
                                        for column in df_identify.columns:
                                            if df_identify.loc[0, column] not in ['price', 'period']:
                                                if df_identify.loc[1, column] == 1:
                                                    current_value = df_identify.loc[2, column]
                                                    if not pd.isna(current_value) and current_value != digit:
                                                        df_unit_source_stat.loc['after', 'error'].append('unit_overwrite')
                                                        after_unit_overwrite=1
                                                    df_identify.loc[2, column] = digit
            
            
            
            too_many_str_top_units=0
            y_in_str_top_units=0
            top_unit_overwrite=0
            
            if  top_left_units:
                df_unit_source_stat.loc['top', 'source_dum']=1
                
                potential_source_of_unit['top']=top_left_units
                
                df_unit_source_stat.at['top', 'values']=top_left_units
                
                
                top_left_units_type = [0 if type(t) == float else 1 if type(t) == str else -1 for t in top_left_units]
                
                if len(set(top_left_units_type)) > 1:
                    df_unit_source_stat.loc['top', 'error'].append('multiple_types')
                if -1 in top_left_units_type:
                    df_unit_source_stat.loc['top', 'error'].append('unknown_types')
                if 0 in top_left_units_type:
                    df_unit_source_stat.loc['top', 'error'].append('float_type')
                    
                    
                
                if len(top_left_units)>2:
                    too_many_str_top_units=1
                    df_unit_source_stat.loc['top', 'error'].append('too_many_str_units')
                
                if len(top_left_units)<=2:
                    
                   if  all(isinstance(x, str) for x in top_left_units):
                           
                        
                        unique_strings=top_left_units
                        
                        if any('y' in s for s in unique_strings):
                            df_unit_source_stat.loc['top', 'error'].append('y_in_str_units')
                            y_in_str_top_units=1
                        else:
                            # Loop over values in unique_strings
                            for value in unique_strings:
                                if value in ['a1', 'a2', 'a3']:
                                    # Extract the digit from the value
                                    digit = int(value[1])
                                    # Update the second row of df_identify for all columns except 'price' or 'period'
                                    for column in df_identify.columns:
                                        if df_identify.loc[0, column] not in ['price', 'period']:
                                            current_value = df_identify.loc[2, column]
                                            if not pd.isna(current_value) and current_value != digit:
                                                df_unit_source_stat.loc['top', 'error'].append('unit_overwrite')
                                                top_unit_overwrite=1
                                            df_identify.loc[2, column] = digit
                                elif value in ['s1', 's2', 's3', 'd1', 'd2', 'd3']:
                                    # Extract the prefix and the digit from the value
                                    prefix = value[0]
                                    digit = int(value[1])
                                    # Update the second row of df_identify based on prefix and digit
                                    for column in df_identify.columns:
                                        if df_identify.loc[0, column] not in ['price', 'period']:
                                            if prefix == 'd' and df_identify.loc[1, column] == 1:
                                                # Dollar column
                                                current_value = df_identify.loc[2, column]
                                                if not pd.isna(current_value) and current_value != digit:
                                                    df_unit_source_stat.loc['top', 'error'].append('unit_overwrite')
                                                    top_unit_overwrite=1
                                                df_identify.loc[2, column] = digit
                                            elif prefix == 's' and df_identify.loc[1, column] == 0:
                                                # Share column
                                                current_value = df_identify.loc[2, column]
                                                if not pd.isna(current_value) and current_value != digit:
                                                    df_unit_source_stat.loc['top', 'error'].append('unit_overwrite')
                                                    top_unit_overwrite=1
                                                df_identify.loc[2, column] = digit
                                elif len(value) == 4:
                                    # Split the value into two 2-character strings
                                    part1, part2 = value[:2], value[2:]
                                    for part in [part1, part2]:
                                        if part in ['a1', 'a2', 'a3']:
                                            digit = int(part[1])
                                            for column in df_identify.columns:
                                                if df_identify.loc[0, column] not in ['price', 'period']:
                                                    current_value = df_identify.loc[2, column]
                                                    if not pd.isna(current_value) and current_value != digit:
                                                        df_unit_source_stat.loc['top', 'error'].append('unit_overwrite')
                                                        top_unit_overwrite=1
                                                    df_identify.loc[2, column] = digit
                                        elif part in ['s1', 's2', 's3']:
                                            digit = int(part[1])
                                            for column in df_identify.columns:
                                                if df_identify.loc[0, column] not in ['price', 'period']:
                                                    if df_identify.loc[1, column] == 0:
                                                        current_value = df_identify.loc[2, column]
                                                        if not pd.isna(current_value) and current_value != digit:
                                                            df_unit_source_stat.loc['top', 'error'].append('unit_overwrite')
                                                            top_unit_overwrite=1
                                                        df_identify.loc[2, column] = digit
                                        elif part in ['d1', 'd2', 'd3']:
                                            digit = int(part[1])
                                            for column in df_identify.columns:
                                                if df_identify.loc[0, column] not in ['price', 'period']:
                                                    if df_identify.loc[1, column] == 1:
                                                        current_value = df_identify.loc[2, column]
                                                        if not pd.isna(current_value) and current_value != digit:
                                                            df_unit_source_stat.loc['top', 'error'].append('unit_overwrite')
                                                            top_unit_overwrite=1
                                                        df_identify.loc[2, column] = digit
                
                        
                
                    
            try:
                df_cut2=df_cut2.map(dollar_dropper)
            except Exception as e:
                self.flow_dic['error_term_re']="dollar_dropper"
                self.flow_dic['error_term_re_e']=str(e)
                return (self.flow_dic,self.df_output2)
                
            unit_healthy=np.nan
            df_output=pd.DataFrame()
            id_list=list(df_cut2['id'].dropna())
            roles = df_identify.iloc[0, :5].to_dict()
            
            if len(roles)!=5:
                self.flow_dic['self_term_re']='not_all_roles_found'
                return (self.flow_dic,self.df_output2)
            
            rename_dict = {int(k): v for k, v in roles.items()}
            
            if {1,2,3}==set(id_list):
                rank1=list(df_cut2[df_cut2['id']==1]['rank'])
                rank2=list(df_cut2[df_cut2['id']==2]['rank'])
                rank3=list(df_cut2[df_cut2['id']==3]['rank'])
                if len(rank1)==1 and len(rank2)==1 and len(rank3)==1:
                    df_output=df_cut2.copy()
            
            if {1,2,3,4}==set(id_list):
                
                rank1=list(df_cut2[df_cut2['id']==1]['rank'])
                rank2=list(df_cut2[df_cut2['id']==2]['rank'])
                rank3=list(df_cut2[df_cut2['id']==3]['rank'])
                rank4=list(df_cut2[df_cut2['id']==4]['rank'])
                
                rank_len_list3=[len(rank1),len(rank2), len(rank3)]
                
                if len(rank1)==1 and len(rank2)==1 and len(rank3)==1 and len(rank4)==1:
                    s={rank1[0],rank2[0],rank3[0]}
                    if s=={0,0,0} and rank4[0]==0:
                        df_output=df_cut2.copy()
                        
                    if s=={1,1,1} and rank4[0]==0:
                        df_output=df_cut2.copy()
                        
                    if s=={0,0,1} and rank4[0]==0:
                        df_temp=df_cut2[df_cut2['rank']==1].copy()
                        x=list(df_temp[0])[0]
                        x=single_digit_or_letter_in_parenth_remover(x)
                        x=ends_text_strip(x) 
                        
                        x_id=df_temp.index[0]
                        x_sc=label_score(x)

                        
                        if pd.isna(x_sc) or x_sc>0:
                            df_output=df_cut2.copy()
                            df_output['table_id']=np.nan
                            df_output['table_score']=np.nan
                            df_output.loc[x_id,'table_id']=1
                            df_output.loc[x_id,'table_score']=x_sc
                        if x_sc<0:
                            df_output=df_cut2.copy()
                            df_output['table_id']=np.nan
                            df_output['table_score']=np.nan
                            df_output.loc[x_id,'table_id']=2
                            df_output.loc[x_id,'table_score']=x_sc
                            
                        for idx in [1,2,3,4]:
                            if pd.isna(df_output.loc[df_output['id'] == idx, 'table_id']).all():
                                df_output.loc[df_output['id'] == idx, 'table_id'] = -1

                        
                if len(rank1)==2 and len(rank2)==2 and len(rank3)==2 and len(rank4)==2:
                    if set(rank1)=={1,2} and set(rank2)=={1,2} and set(rank3)=={1,2} and set(rank4)=={1,2}:
                        #probably complete half table
                        label1=list(set(df_cut2[df_cut2['rank']==1][0]))
                        
                        label2=list(set(df_cut2[df_cut2['rank']==2][0]))
                        
                        #label1_proc=[footnote_remover(t,cand_footnotes_in_text_after) for t in label1]
                        #label2_proc=[footnote_remover(t,cand_footnotes_in_text_after) for t in label2]
                        
                        label1_proc=[single_digit_or_letter_in_parenth_remover(t) for t in label1]
                        label2_proc=[single_digit_or_letter_in_parenth_remover(t) for t in label2]
                        
                        label1_proc=[ends_text_strip(t) for t in label1_proc]
                        label2_proc=[ends_text_strip(t) for t in label2_proc]
                        
                        label1_proc=list(set(label1_proc))
                        label2_proc=list(set(label2_proc))
                        
                        if len(label1_proc)==1 and len(label2_proc)==1:
                           df_output=df_cut2.copy()
                           
                           df_output['table_id']=np.nan
                           df_output['table_score']=np.nan
                            
                           lab1=label1_proc[0]
                           lab2=label2_proc[0]
                           
                           lab1_score=label_score(lab1)
                           lab2_score=label_score(lab2)
                           
                           if lab1_score-lab2_score>=2:
                              df_output.loc[df_output['rank'] == 1, 'table_id'] = 1
                              df_output.loc[df_output['rank'] == 2, 'table_id'] = 2
                              
                              df_output.loc[df_output['rank'] == 1, 'table_score'] = lab1_score
                              df_output.loc[df_output['rank'] == 2, 'table_score'] = lab2_score
                              
                           if lab2_score-lab1_score>=2:
                               df_output.loc[df_output['rank'] == 2, 'table_id'] = 1
                               df_output.loc[df_output['rank'] == 1, 'table_id'] = 2
                               
                               df_output.loc[df_output['rank'] == 1, 'table_score'] = lab1_score
                               df_output.loc[df_output['rank'] == 2, 'table_score'] = lab2_score
                           
                    
                        
                if len(rank1)==2 and len(rank2)==2 and len(rank3)==2 and len(rank4)==1:
                    if set(rank1)=={1,2} and set(rank2)=={1,2} and set(rank3)=={1,2} and set(rank4)=={0}:
                        #probably complete half table
                        label1=list(set(df_cut2[df_cut2['rank']==1][0]))
                        
                        label2=list(set(df_cut2[df_cut2['rank']==2][0]))
                        
                        #label1_proc=[footnote_remover(t,cand_footnotes_in_text_after) for t in label1]
                        #label2_proc=[footnote_remover(t,cand_footnotes_in_text_after) for t in label2]
                        
                        label1_proc=[single_digit_or_letter_in_parenth_remover(t) for t in label1]
                        label2_proc=[single_digit_or_letter_in_parenth_remover(t) for t in label2]
                        
                        label1_proc=[ends_text_strip(t) for t in label1_proc]
                        label2_proc=[ends_text_strip(t) for t in label2_proc]
                        
                        label1_proc=list(set(label1_proc))
                        label2_proc=list(set(label2_proc))
                        
                        if len(label1_proc)==1 and len(label2_proc)==1:
                           df_output=df_cut2.copy()
                           
                           df_output['table_id']=np.nan
                           df_output['table_score']=np.nan
                            
                           lab1=label1_proc[0]
                           lab2=label2_proc[0]
                           
                           lab1_score=label_score(lab1)
                           lab2_score=label_score(lab2)
                           
                           if lab1_score-lab2_score>=2:
                              df_output.loc[df_output['rank'] == 1, 'table_id'] = 1
                              df_output.loc[df_output['rank'] == 2, 'table_id'] = 2
                              df_output.loc[df_output['id'] == 4, 'table_id'] = 1
                              
                              df_output.loc[df_output['rank'] == 1, 'table_score'] = lab1_score
                              df_output.loc[df_output['rank'] == 2, 'table_score'] = lab2_score
                              df_output.loc[df_output['id'] == 4, 'table_score'] = lab1_score
                              
                              
                           if lab2_score-lab1_score>=2:
                               df_output.loc[df_output['rank'] == 2, 'table_id'] = 1
                               df_output.loc[df_output['rank'] == 1, 'table_id'] = 2               
                               df_output.loc[df_output['id'] == 4, 'table_id'] = 1
                               
                               df_output.loc[df_output['rank'] == 1, 'table_score'] = lab1_score
                               df_output.loc[df_output['rank'] == 2, 'table_score'] = lab2_score
                               df_output.loc[df_output['id'] == 4, 'table_score'] = lab2_score
                             
                if max(rank_len_list3)==2 and min(rank_len_list3)==1 and len(rank4)==1:
                    

                        
                        label_dict={}
                        df_temp=df_cut2[df_cut2['id']<=3].copy()
                        for i in  df_temp.index:
                            if df_temp.loc[i,'rank']==0:
                                continue 
                            x=df_temp.loc[i,0]
                            x=single_digit_or_letter_in_parenth_remover(x)
                            x=ends_text_strip(x) 
                            if x not in label_dict.keys():
                                label_dict[x]=[i]
                            else:
                                label_dict[x].append(i)
                                
            
                            
                        check1=0
                        check2=1
                        
                        if len(label_dict.keys())==2:
                            check1=1
                        
                        for w in label_dict.values():
                            if len(w)!=len(set(w)):
                                check2=0
                        
                        
                        if check1==1 and check2==1:
                            label_dict_score={}
                            for q in label_dict.keys():
                                sc=label_score(q)
                                label_dict_score[q]=sc
                                
                            keys=list(label_dict_score.keys())
                            
                            sc1=label_dict_score[keys[0]]
                            sc2=label_dict_score[keys[1]]
                            
                            if sc1-sc2>=2:
                                df_output=df_cut2.copy()
                                df_output['table_id']=np.nan
                                df_output['table_score']=np.nan
                                
                                
                                
                                df_output.loc[label_dict[keys[0]], 'table_id'] = 1
                                df_output.loc[label_dict[keys[1]], 'table_id'] = 2
                                df_output.loc[df_output['id'] == 4, 'table_id'] = -1
                                
                                df_output.loc[label_dict[keys[0]], 'table_score'] = sc1
                                df_output.loc[label_dict[keys[1]], 'table_score'] = sc2
                                
                                
                                for idx in [1,2,3,4]:
                                    if pd.isna(df_output.loc[df_output['id'] == idx, 'table_id']).all():
                                        df_output.loc[df_output['id'] == idx, 'table_id'] = -1
           
                            if sc2-sc1>=2:
                                df_output=df_cut2.copy()
                                df_output['table_id']=np.nan
                                df_output['table_score']=np.nan
                                
                                
                                
                                df_output.loc[label_dict[keys[1]], 'table_id'] = 1
                                df_output.loc[label_dict[keys[0]], 'table_id'] = 2
                                df_output.loc[df_output['id'] == 4, 'table_id'] = -1
                                
                                df_output.loc[label_dict[keys[0]], 'table_score'] = sc1
                                df_output.loc[label_dict[keys[1]], 'table_score'] = sc2  
                                
                                
                                for idx in [1,2,3,4]:
                                    if pd.isna(df_output.loc[df_output['id'] == idx, 'table_id']).all():
                                        df_output.loc[df_output['id'] == idx, 'table_id'] = -1
                                        
  
            if df_output.shape[0]==0:
                self.flow_dic['self_term_re']='df_output_not_created' 
                return (self.flow_dic,self.df_output2)
            
               
            if df_output.shape[0]>0:
                if 'table_id' not in df_output.columns:
                    df_output['table_id']=np.nan
                    df_output.loc[df_output['id'] <=4, 'table_id'] = 0
                    
                if 'table_score' not in df_output.columns:
                    df_output['table_score']=np.nan
                
                dollars = df_identify.iloc[1, :5].to_dict()
                dollars_dict = {int(k): v for k, v in dollars.items()}
                
                # Initialize a new row with NaN or any default value
                new_row = {col: np.nan for col in df_output.columns}
                
                # Replace values in the new row based on dollars_dict
                for col, d in dollars_dict.items():
                    if col in df_output.columns:  # Check if the column index exists in df_output
                        new_row[col] = d
                
                # Append the new row to df_output
                # We first make a DataFrame from the new_row dictionary and then append
                new_row_df = pd.DataFrame([new_row], index=[-1])  # Creating a DataFrame with an index of -1
                df_output = pd.concat([df_output, new_row_df], ignore_index=False)
                
                
                
                u=df_identify.iloc[2, :5].to_dict()
                u_dict = {int(k): v for k, v in u.items()}
                
                new_row = {col: np.nan for col in df_output.columns}
                
                for col, d in u_dict.items():
                    if col in df_output.columns:  # Check if the column index exists in df_output
                        new_row[col] = d
                        
                new_row_df = pd.DataFrame([new_row], index=[-2])  # Creating a DataFrame with an index of -1
                df_output = pd.concat([df_output, new_row_df], ignore_index=False)
                
                df_output['beg_date']=np.nan
                df_output['end_date']=np.nan
                
                for m in [1,2,3]:
                    b=monthly_interval_dates_converted[m][0] 
                    df_output.loc[df_output['id']==m,'beg_date']=b 
                    
                    e=monthly_interval_dates_converted[m][1] 
                    df_output.loc[df_output['id']==m,'end_date']=e
                
                try:
                    df_output_pos_footnote=df_output.map(extract_single_digit_or_letter_in_parenth)
                except Exception as e:
                    self.flow_dic['error_term_re']="extract_single_digit_or_letter_in_parenth"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
                
               
                
                subset = df_output.iloc[1:, 1:]  
                
                try:
                    processed_subset = subset.applymap(single_digit_or_letter_in_parenth_remover)
                except Exception as e:
                    self.flow_dic['error_term_re']="single_digit_or_letter_in_parenth_remover"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
                
                try:
                    processed_subset = processed_subset.applymap(general_parenth_remover)
                except Exception as e:
                    self.flow_dic['error_term_re']="general_parenth_remover"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
                
                
                try:
                    processed_subset = processed_subset.applymap(unit_remover)
                except Exception as e:
                    self.flow_dic['error_term_re']="unit_remover"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
                
                try:
                    processed_subset = processed_subset.applymap(star_remover)
                except Exception as e:
                    self.flow_dic['error_term_re']="star_remover"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
                
                
                try:
                    processed_subset = processed_subset.applymap(other_missing_creater)
                except Exception as e:
                    self.flow_dic['error_term_re']="other_missing_creater"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
                

            
                # Reassign the processed data back to the original DataFrame's subset
                df_output.iloc[1:, 1:] = processed_subset
            
                try:
                    self.df_output2=df_output.map(convert_to_number)
                except Exception as e:
                    self.flow_dic['error_term_re']="convert_to_number"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
                
                
                subset = self.df_output2.iloc[1:, 1:]  
                try:
                    processed_subset = subset.applymap(inner_cell_health_checker)
                except Exception as e:
                    self.flow_dic['error_term_re']="inner_cell_health_checker"
                    self.flow_dic['error_term_re_e']=str(e)
                    return (self.flow_dic,self.df_output2)
               
                all_inner_cells_are_healthy = 1 if processed_subset.all().all() else 0
                self.flow_dic['inner_cell_health']=all_inner_cells_are_healthy
                l=list(df_unit_source_stat['error'])
                ll=[len(x) for x in l]
                s=set(ll)
                
                
                unit_source=list(df_unit_source_stat[df_unit_source_stat['source_dum']==1].index)
                
                if s=={0}:
                    unit_healthy=1
                else:
                    unit_healthy=0
                
                self.flow_dic['unit_source']=unit_source
                self.flow_dic['unit_healthy']=unit_healthy
                
                
                if unit_healthy==0:
                    self.flow_dic['self_term_re']='unhealthy_unit'
                    return (self.flow_dic,self.df_output2)
                if all_inner_cells_are_healthy==0:
                    self.flow_dic['self_term_re']='unhealthy_inner_cell'
                    return (self.flow_dic,self.df_output2)
                    
                new_row = pd.Series(np.nan, index=self.df_output2.columns)
                for col, role in roles.items():
                    if role == 'period':
                        new_row[col] = 0
                    elif role == 'tot':
                        new_row[col] = 1
                    elif role == 'price':
                        new_row[col] = 2
                    elif role == 'part':
                        new_row[col] = 3
                    elif role == 'remain':
                        new_row[col] = 4
                    
                    self.df_output2.loc[-3] = new_row
                return (self.flow_dic,self.df_output2)

        except ExtractionError as e:
            # Return the error state from the exception
            return (e.flow_dic, e.df_output2)
        except Exception as e:
            # Handle unexpected errors
            self.flow_dic['error_term_re']="general"
            self.flow_dic['error_term_re_e']=str(e)
            return (self.flow_dic,self.df_output2)
    


# Test:
file_link_filing='https://www.sec.gov/Archives/edgar/data/789019/000156459022035087/msft-10q_20220930.htm'

# other tests:
file_link_filing="https://www.sec.gov/Archives/edgar/data/320193/000032019322000070/aapl-20220625.htm"
out=RepurchaseExtractor(file_link_filing)       
            
        
        
                       
                       
                   
                   
                   
                   
                
                
                
                
                
      
