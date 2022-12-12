import argparse
import os
import pathlib

import pandas as pd
from file_parser import read_rules_from_file


def analyse_rule(rule: pd.Series, all_tests: dict) -> list:
    """
    Function to analyze the tests inside a rule. For each tests in the rule, the function will check if the tests is in
    second parameter dictionary to see if it has already been found. If it hasn't, it will create an entry that will
    contain the test type, the total number of times the test has been found, the number of times it has been found in
    active rules, the number of times it has been found in inactive rules, and which rules and building blocks it has
    been found in.
    :param rule: Rule to analyze
    :param all_tests: dictionary that contains all previous tests analyzed
    :return: a list of the tests this rule contained.
    """
    #tests = rule['Tests'][2:-1].split('\\n')
    tests = rule['Tests']
    for j, test in enumerate(tests):
        if test not in all_tests.keys():
            test_type = ""
            if test.lower().find("payload") != -1:
                test_type = "Payload"
            elif test.lower().find("are contained") != -1:
                test_type = "Reference Set"
            elif test.lower().find("aql filter") != -1:
                test_type = "AQL Query"
            elif test.lower().find("matches the following") != -1:
                test_type = "Regex"

            all_tests[test] = {'Type': test_type,
                               'Total_Count': 0,
                               'Active_count': 0,
                               'Inactive_count': 0,
                               'BB': [],
                               'Rules': []}

        all_tests[test]['Total_Count'] += 1
        if rule["Enabled"]:
            all_tests[test]['Active_count'] += 1
        else:
            all_tests[test]['Inactive_count'] += 1

        if rule['Building Block']:
            all_tests[test]['BB'].append(rule['Rule Name'])
        else:
            all_tests[test]['Rules'].append(rule['Rule Name'])
    return tests


def parse_rules_log(file: str, mails: list = None, rules: list = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Function to analyze a rule_log's rules and tests in order to facilitate the system's auditioning.
    :param file: log file to analyze
    :param mails: users/mails to check. If the string can be found in the user the rule belongs to, it will be analyzed
    :param rules: list of rules to analyze. The names of the rules found in the file will be compared to see if there
    is any match to any of the names provided.
    :return: a tuple of three panda data frames, the first one which contains the rule analysis, the second one the test
    analysis and the third on the data used to obtain the first two.
    """
    data = read_rules_from_file(file)

    # data_trimmed = data[['Rule Name', 'Enabled', 'Owner', 'Tests', 'RESP: Add to Reference Set']]

    if mails is not None:
        data = data[[any([rule_owner.find(owner) != -1 for owner in mails]) for rule_owner in data.Owner]]

    if rules is not None:
        # For each row, check if the rule name is in the list of rules we want to find.
        data = data[[any([rule_name.find(rule) != -1 for rule in rules]) for rule_name in data['Rule Name']]]
        # Equivalent code
        '''
        rules_wanted = []
        for rule_name in data['Rule Name']:
            for rule in rules:
                if rule_name.find(rule) != -1:
                    rules_wanted.append(True)
                    break
        data = data[rules_wanted]
        '''
    if data.empty:
        print(f"no data found with the criteria {mails=} and {rules=}")
        return None, None, None
    all_tests = {}
    for i in data.index:
        data.at[i, 'Tests'] = analyse_rule(data.loc[i], all_tests)

    data.insert(4, "Test_count", [len(data.loc[rule, 'Tests']) for rule in data.index])
    rules_RESP_RS = data[~data['RESP: Add to Reference Set'].isnull()]
    rules_RESP_RS = rules_RESP_RS[['Rule Name', 'Test_count', 'Enabled', 'Tests', 'RESP: Add to Reference Set']]
    df = pd.DataFrame.from_dict(all_tests, orient='index')
    return rules_RESP_RS, df, data


def df_to_txt(df: pd.DataFrame, output: str):
    with open(output, "w") as file_writer:
        for row in df.index:
            row_data = df.loc[row]
            for column in df.columns:
                if column != 'Tests':
                    file_writer.write(f"{column}{' '*(30 - len(column))}: {row_data[column]}\n")
                else:
                    tests = '\n\t'.join(row_data[column])
                    file_writer.write(f"{column}{' '*(30 - len(column))}: \n\t{tests}\n")
            file_writer.write("\n\n")


def main():
    parser = argparse.ArgumentParser(description='program to parse the log output of QRadar, '
                                                 'counting the number of times each test appears and classifying them, '
                                                 'in order to facilitate the analysis of the situation. And the number '
                                                 'of rules that affect a reference set with the ammount of tests they '
                                                 'have.')
    parser.add_argument('-f', '--file', required=False, type=pathlib.Path, help='log file to analyze.')
    parser.add_argument('-o', '--output', required=False, default="rule_log_analysis", type=pathlib.Path,
                        help='Output file to write the results to, without the filetype. By default it will be saved in'
                             ' the working directory under the name \"rule_log_analysis\"')
    parser.add_argument('-m', "--mails", nargs='+', default=None,
                        help='List of mails of the users whose rules you want to analyze. By default it is left blank, '
                             'to analyze all users. The match is not complete, only parcial, so adding "ibm" to the '
                             'list of mails with add any email/owner that contains ibm in the name. It can also be a '
                             'file that contains different emails in each line.')
    parser.add_argument('-r', "--rules", nargs='+', default=None,
                        help="Specify a list of rules to analyze; it doesn't have to be a full name, it can be generic."
                             " If the name have spaces, remember to encase it between \". Alternatively, it can be a "
                             "file containing a list of names to search for, one name per line.")
    parser.add_argument('-R', "--readable", action='store_true',
                        help='Changes the output to just a simple txt file where the rules will be loaded in a more '
                             'readable manner')

    args = parser.parse_args()
    if args.file is None:
        file = ""
        for _file in [f for f in os.listdir() if os.path.isfile(f)]:
            if _file.endswith(".tsv"):
                file = _file

        if file == "":
            print("No file ending in tsv found.")
    else:
        file = args.file

    rules = args.rules
    if rules is not None and len(rules) == 1 and os.path.isfile(rules[0]):
        with open(rules[0]) as file_reader:
            rules = [line.strip() for line in file_reader.readlines()]

    mails = args.mails
    if mails is not None and len(mails) == 1 and os.path.isfile(mails[0]):
        with open(mails[0]) as file_reader:
            mails = [line.strip() for line in file_reader.readlines()]

    try:
        rules_RESP_RS, df, data = parse_rules_log(file, mails, rules)
        if rules_RESP_RS is None:
            return
    except KeyError as e:
        print(f"\n{type(e)}:{e}")
        print("Seems like there was an error parsing the rule file. Please make sure that the following columns are"
              "present in the file: 'Tests', 'Rule Name', 'Test_count', 'Enabled', 'Tests', 'RESP: Add to Reference Set'")
        return
    args.output = str(args.output)
    if args.readable:
        print(f"[INFO] :: Writing legible info of their rules to file: {args.output + '.txt'}")
        df_to_txt(data, args.output+".txt")
    else:
        df.to_csv(args.output+".tsv", sep="\t")
        '''
        with pd.ExcelWriter(args.output+".xlsx") as excel_writer:
            print(f"[INFO] :: writing analysis results to excel file: {args.output+'.xlsx'}")
            rules_RESP_RS.to_excel(excel_writer, sheet_name='Rules Analysis')
            df.to_excel(excel_writer, sheet_name='Test Analysis')
        '''


if __name__ == '__main__':
    main()
