import pandas as pd
import numpy as np
from datetime import datetime

def main():
    #
    username = input('Please enter username for filepath: ')
    print('Please use the following query for RE Export and choose the emails you want included in the markup:'
          ' RE> Query > MatthewK2021 > Monthly Email Test Group Markup' )
    # File name declared for export
    file_name = input('Please enter filename of RE export outlined above: ')
    df_master = pd.read_csv(fr'C:\Users\mkerr\SCOTTISH CATHOLIC INTERNATIONAL AID FUND\Data Insights - python scripts\Monthly Email Test Group Creation\input\{file_name}.csv')
    df_master.columns = df_master.columns.str.replace(" ", "_").str.replace(":", "").str.lower()
    len_before = len(set(df_master.constituent_id))
    df_master_for_joining = df_master
    # Remove any pending, bounced or failed in markup
    df_master = df_master.loc[df_master.action_status != 'Pending'].copy()
    df_master = df_master.loc[df_master.action_status != 'Bounced'].copy()
    df_master = df_master.loc[df_master.action_status != 'Failed'].copy()
    # Take a list of all unique cons_ids
    cons_ids = set(df_master.constituent_id)
    # Declare a list for actions constituent can take with email
    action_list = ['Opened', 'Clicked', 'Responded', 'Gave']
    df_score = pd.DataFrame()
    for con in cons_ids:
        # Take a dataframe for specific constituent
        df_con = df_master.loc[df_master.constituent_id == con].copy()
        # Checks number of emails user got
        number_sent = len(df_con)
        # Checks the lengths of the dataframe where user actioned the email
        df_con_opened = df_con.loc[df_con.action_status.isin(action_list)]
        opened_count = len(df_con_opened)
        # Score is given to the user based on how many emails they've taken an action on
        open_rate = (opened_count / number_sent) * 100
        score = 0
        score_type = "didn't open an email"
        # If they have been sent more than 5 emails they are eligible for the highest score of 100
        if number_sent >= 5:
            if open_rate == 100:
                score = 100
                score_type = '5+ emails and 100% open rate'
            if open_rate >= 75 and open_rate <= 99:
                score = 98
                score_type = '5+ emails and 75% - 99% open rate'
            if open_rate >= 50 and open_rate <= 74:
                score = 96
                score_type = '5+ emails and 50% - 74% open rate'
            if open_rate >= 25 and open_rate <= 49:
                score = 94
                score_type = '5+ emails and 25% - 49% open rate'
            if open_rate >= 1 and open_rate <= 24:
                score = 92
                score_type = '5+ emails and 1% - 24% open rate'
            if open_rate == 0:
                score = 87
                score_type = '5+ emails and 0% open rate'
        # Section is for people who have received less emails than others
        if number_sent >= 2 and number_sent <= 4:
            if open_rate == 100:
                score = 99
                score_type = '2 - 4 emails and 75% - 99% open rate'
            if open_rate >= 75 and open_rate <= 99:
                score = 97
                score_type = '2 - 4  emails and 75% - 99% open rate'
            if open_rate >= 50 and open_rate <= 74:
                score = 95
                score_type = '2 - 4  emails and 50% - 74% open rate'
            if open_rate >= 25 and open_rate <= 49:
                score = 93
                score_type = '2 - 4  emails and 25% - 49% open rate'
            if open_rate >= 1 and open_rate <= 24:
                score = 91
                score_type = '2 - 4  emails and 1% - 24% open rate'
            if open_rate == 0:
                score = 88
                score_type = '2 - 4  emails and 0% open rate'
        if number_sent == 1:
            if open_rate == 100:
                score = 90
                score_type = '1  email and 100% open rate'
            if open_rate == 0:
                score = 89
                score_type = '1 email and 0% open rate'
        df_score = df_score.append([[con, open_rate, opened_count, number_sent, score, score_type]])
    df_score.reset_index(inplace=True, drop=True)
    # df_score is renamed to reflect columns added
    df_score.rename({0: 'constituent_id',
                     1: 'open_rate',
                     2: 'open_count',
                     3: 'number_sent',
                     4: 'email_score',
                     5: 'score_type'}, axis=1, inplace=True)
    # df master for joining above is merged with df score
    df_joined = pd.merge(df_master_for_joining, df_score, on='constituent_id', how='left')
    # df export is set up for importing
    df_export = df_joined.drop(['action_specific_attributes_email_subject_import_id',
                                'action_specific_attributes_email_subject_description',
                                'action_specific_attributes_email_subject_date',
                                'action_specific_attributes_email_subject_comments', 'action_status', 'action_date'],
                               axis=1)
    df_export.sort_values(['email_score', 'constituent_date_added'], ascending=False, inplace=True)
    df_export.drop_duplicates(inplace=True)
    df_export.reset_index(inplace=True)
    df_export['index_vale'] = df_export.index
    df_export['updated_test_group'] = '0'
    df_export['updated_test_group'] = np.where(df_export.index % 2 == 0, 'A', df_export['updated_test_group'])
    df_export['updated_test_group'] = np.where(df_export.index % 2 != 0, 'B', df_export['updated_test_group'])
    print(f'Len Before: {len_before} | Len After: {len(df_export)}')
    df_export['constituent_specific_attributes_monthly_newsletter_email_description'] = df_export['updated_test_group']
    df_export['constituent_specific_attributes_monthly_newsletter_email_comments'] = df_export['score_type']
    df_export['constituent_specific_attributes_monthly_newsletter_email_comments'].fillna('No Response Info', inplace=True)
    df_for_import = df_export[['constituent_id',
                               'constituent_specific_attributes_monthly_newsletter_email_import_id',
                               'constituent_specific_attributes_monthly_newsletter_email_description',
                               'constituent_specific_attributes_monthly_newsletter_email_date',
                               'constituent_specific_attributes_monthly_newsletter_email_comments']].copy()
    comment_list = set(df_for_import.constituent_specific_attributes_monthly_newsletter_email_comments)
    for comment in comment_list:
        df_comment = df_for_import.loc[df_for_import.constituent_specific_attributes_monthly_newsletter_email_comments == comment].copy()
        print(f'For {comment}')
        print(f'Len A: {len(df_comment.loc[df_comment.constituent_specific_attributes_monthly_newsletter_email_description == "A"].copy())}')
        print(f'Len B: {len(df_comment.loc[df_comment.constituent_specific_attributes_monthly_newsletter_email_description == "B"].copy())}\n')
    df_for_import['attr_category'] = 'Monthly Newsletter Email'
    df_for_import.rename({'constituent_specific_attributes_monthly_newsletter_email_import_id' : 'attr_imp_id',
                                          'constituent_specific_attributes_monthly_newsletter_email_description' : 'attr_desc',
                                          'constituent_specific_attributes_monthly_newsletter_email_date' : 'att_date',
                                          'constituent_specific_attributes_monthly_newsletter_email_comments' : 'attr_comments'}, axis=1, inplace=True)
    df_for_import = df_for_import[['constituent_id', 'attr_imp_id', 'attr_category', 'attr_desc', 'att_date', 'attr_comments']]
    current_date_for_export = datetime.today().strftime('%Y%m%d')
    df_for_import.to_csv(fr'C:\Users\mkerr\SCOTTISH CATHOLIC INTERNATIONAL AID FUND\Data Insights - python scripts\Monthly Email Test Group Creation\output\monthly_test_group_import_{current_date_for_export}.csv', sep=',', index=False)
    print(fr'File exported as: monthly_test_group_import_{current_date_for_export}.csv')
    print('Please use the following path for import: RE> Import > Constituent Attribute > Single Attribute Import - AD' )

main()