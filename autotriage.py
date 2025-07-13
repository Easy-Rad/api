import json
from tokenise import tokenise_request


with open('data/labels.json', 'r') as f:
    labels = json.load(f)

with open('data/exams.json', 'r') as f:
    exams = json.load(f)

def autotriage(
    modality: str,
    requested_exam: str,
    normalised_exam: str,
    patient_age: int,
    egfr: int | None,
) -> dict | None:
    if modality not in labels:
        return None
    tokenised = tokenise_request(requested_exam) if requested_exam else tokenise_request(normalised_exam)
    try:
        code = labels[modality][tokenised if (not requested_exam) or (tokenised in labels[modality]) else tokenise_request(normalised_exam)]
    except KeyError:
        return None
    if code=='Q25' and (patient_age >= 80 or egfr is not None and egfr < 30): code='Q25T' # Barium-tagged CT colonography
    body_part, exam = exams[modality][code]
    return dict(
        request=dict(
            exam=requested_exam,
            normalised_exam=normalised_exam,
            tokenised=tokenised,
            patient_age=patient_age,
            egfr=egfr,
        ),
        body_part=body_part,
        code=code,
        exam=exam,
    )