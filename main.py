import click
import pandas as pd
import pathlib


PERSONAL_DOMAINS = [
    "@gmail.com",
    "@googlemail.com",
    "@outlook.com",
    "@hotmail.com",
    "@live.com",
    "@msn.com",
    "@yahoo.com",
    "@ymail.com",
    "@rocketmail.com",
    "@icloud.com",
    "@me.com",
    "@mac.com",
    "@aol.com",
    "@love.com",
    "@zoho.com",
    "@mail.com",
    "@usa.com",
    "@consultant.com",
    "@engineer.com",
    "@techie.com",
    "@gmx.com",
    "@gmx.net",
    "@gmx.de",
    "@proton.me",
    "@protonmail.com",
    "@tutanota.com",
    "@tutanota.de",
    "@tutamail.com",
    "@tuta.io",
    "@yandex.com",
    "@yandex.ru",
    "@mail.ru",
    "@bk.ru",
    "@inbox.ru",
    "@list.ru",
    "@fastmail.com",
    "@fastmail.fm",
    "@fastmail.net",
    "@mailbolt.com",
    "@pm.me",
]

def is_personal_email(recipient_status: str) -> int:
    if pd.isnull(recipient_status):
        return 0

    recipient_status_lower = recipient_status.lower()
    return 1 if any(domain in recipient_status_lower for domain in PERSONAL_DOMAINS) else 0


@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file")
def main(input_file: str, output_file: str) -> None:
    csv_to_audit = pathlib.Path(input_file)
    audited_csv = pathlib.Path(output_file)

    encodings_to_try = ['utf-16', 'utf-16-le', 'utf-16-be', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    working_encoding = None
    
    for encoding in encodings_to_try:
        try:
            with open(csv_to_audit, 'r', encoding=encoding) as f:
                f.readline()
                working_encoding = encoding
                break
        except UnicodeDecodeError:
            continue
    
    if not working_encoding:
        print("Error: Could not determine file encoding")
        return
    
    original_csv = None
    parsing_methods = [
        {'encoding': working_encoding, 'engine': 'python'},
        {'encoding': working_encoding, 'engine': 'c'},
        {'encoding': 'latin-1', 'engine': 'python'},
        {'encoding': 'cp1252', 'engine': 'python'},
    ]
    
    for method in parsing_methods:
        try:
            original_csv = pd.read_csv(
                csv_to_audit, 
                encoding=method['encoding'],
                engine=method['engine'],
                header=0,
                quotechar='"',
                skipinitialspace=True,
                on_bad_lines='skip',
                skip_blank_lines=True
            )
            break
        except Exception:
            continue
    
    if original_csv is None:
        print("All parsing methods failed. Trying manual approach...")

        with open(csv_to_audit, 'r', encoding=working_encoding) as f:
            header_line = f.readline().strip()
            headers = [h.strip('"') for h in header_line.split(',')]
            print(f"Manual headers: {headers}")
            original_csv = pd.read_csv(csv_to_audit, encoding=working_encoding, names=headers, skiprows=1, engine='python')

    original_csv.columns = original_csv.columns.str.strip()
    
    if 'recipient_status' not in original_csv.columns:
        print(f"Error: 'recipient_status' column not found. Available columns: {list(original_csv.columns)}")
        return
    
    original_csv["sent_to_personal_acc"] = original_csv["recipient_status"].apply(is_personal_email)

    original_csv.to_excel(audited_csv, index=False)

if __name__ == "__main__":
    main()
