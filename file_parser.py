import pandas as pd


# Print iterations progress
def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', print_end="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        print_end    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
    # Print New Line on Complete
    if iteration == total:
        print()


def test_cleanup(test: str) -> str:
    test = test.strip()
    words = test.split(' ')
    if words[1].lower() == "not":
        words[1] = ''

    if words[0].lower() == "and":
        words[0] = ''
    test = " ".join(words)
    return test


def parse_txt(file: str) -> pd.DataFrame:
    interested_data = ["Rule Name", "Enabled", "Building Block", "Owner",
                       "Response Limiter", "RESP: Add to Reference Set", "Tests"]
    df = pd.DataFrame(columns=interested_data)
    with open(file, "r") as file_reader:
        rule_count = -1
        in_tests = False
        tests = []
        lines = file_reader.readlines()
        lines_num = len(lines)
        for i, line in enumerate(lines):
            print_progress_bar(iteration=i, total=lines_num-1)
            if line == "\n":
                continue

            if in_tests:
                if line.startswith("Notes"):
                    in_tests = False
                    df.at[rule_count, "Tests"] = tests
                    tests = []
                    continue

                try:
                    tests.append(test_cleanup(line))
                except IndexError:
                    print('\r'+' '*200, end="")
                    print(f"\r[WARN] :: Rule: {df.at[rule_count, 'Rule Name']} has a weird test in line {i}")

            if line.startswith("Rule Name"):
                rule_count += 1
                df.at[rule_count, "Rule Name"] = ":".join(line.split(sep=":")[1:]).strip()
            elif line.startswith("Enabled"):
                df.at[rule_count, "Enabled"] = line.split(sep=":")[1].strip() == "True"
            elif line.startswith("Building Block"):
                df.at[rule_count, "Building Block"] = line.split(sep=":")[1].strip() == "True"
            elif line.startswith("Owner"):
                df.at[rule_count, "Owner"] = ":".join(line.split(sep=":")[1:]).strip()
            elif line.startswith("RESP: Add to Reference Set"):
                resp = ":".join(line.split(sep=":")[2:]).strip()
                df.at[rule_count, "RESP: Add to Reference Set"] = resp if resp != "None" else None
            elif line.startswith("Response Limiter"):
                rl = line.split(sep=":")[1].strip()
                df.at[rule_count, "Response Limiter"] = rl if rl != "None" else None
            elif line.startswith("Tests"):
                in_tests = True
    return df


def parse_sv(file: str) -> pd.DataFrame:
    separator = {"tsv": "\t", "csv": ","}[str(file)[-3:]]
    print(f"[INFO] :: Working on file {file}, of the type {str(file)[-3:]}")
    data = pd.read_csv(file, sep=separator)
    rules_test = []
    data_len = len(data)
    for i, rule in enumerate(data.index):
        rule_tests = []
        print_progress_bar(iteration=i, total=data_len-1)
        tests = data.loc[rule, 'Tests'][:-1].split('\n')
        for test in tests:
            try:
                rule_tests.append(test_cleanup(test))
            except IndexError:
                print(f"[WARN] :: Rule: {data.at[i, 'Rule Name']} has a weird test in line {i}")

        rules_test.append(rule_tests)

    # Cleaning the Tests column from \n separated string to list of lists
    data["Tests"] = rules_test
    return data


def read_rules_from_file(file: str) -> pd.DataFrame:
    if str(file).endswith((".tsv", ".csv")):
        return parse_sv(file)
    elif str(file).endswith(".txt"):
        return parse_txt(file)
