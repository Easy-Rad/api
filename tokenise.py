import re

def tokenise_request(s: str) -> str:
    s = re.sub(
        # remove non-alphanumeric characters except for C- and C+
        # remove irrelevant words including modality
        pattern=r'[^\w+-]|(?<!\bC)[+-]|\b(and|or|with|by|left|right|please|GP|CT|MRI?|US|ultrasound|scan|study|protocol|contrast)\b',
        repl=' ',
        string=s,
        flags=re.IGNORECASE|re.ASCII,
    )
    return ' '.join(sorted(re.split(r'\s+', s.lower().strip()))) # remove extra whitespace
