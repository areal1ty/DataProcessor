import datetime
import os
import sys
import pandas as pd

INPUT_PATH = 'input'
OUT_HEADER = ['email', 'create_count', 'read_count', 'update_count', 'delete_count']


def process_logs(date):
    actions = {}

    for i in range(7):
        current_dt = date - datetime.timedelta(days=i)
        file_name = current_dt.strftime('%Y-%m-%d') + '.csv'
        file_path = os.path.join(INPUT_PATH, file_name)

        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path, names=['email', 'action', 'dt'])

        for _, row in df.iterrows():
            email = row['email']
            action = row['action'].lower()

            if email not in actions:
                actions[email] = {'create_count': 0, 'read_count': 0, 'update_count': 0, 'delete_count': 0}

            if action in ['create', 'read', 'update', 'delete']:
                actions[email][f"{action}_count"] += 1

    return actions


def write_aggregated_data(aggregation_date, actions):
    output_date = aggregation_date + datetime.timedelta(days=1)
    output_file = os.path.join('output', f"{output_date.strftime('%Y-%m-%d')}.csv")

    os.makedirs('output', exist_ok=True)

    df_out = pd.DataFrame.from_dict(actions, orient='index').reset_index()
    df_out.columns = ['email'] + OUT_HEADER[1:]

    df_out.to_csv(output_file, encoding='utf-8', index=False)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
        sys.exit(1)

    try:
        input_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    except ValueError:
        print("Incorrect date format. Use YYYY-mm-dd.")
        sys.exit(1)

    user_actions = process_logs(input_date)
    write_aggregated_data(input_date, user_actions)
    print(f"Aggregated data written successfully.")
