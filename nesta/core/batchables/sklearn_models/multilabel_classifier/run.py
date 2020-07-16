import importlib
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import boto3


def retrieve_model():
    sklearn_module, model = os.environ['BATCHPAR_model'].split('.')
    pkg = importlib.import_module(f'sklearn.{sklearn_module}')
    return getattr(pkg, model)


def retrieve_model_kwargs():
    kwargs = {}
    for k, v in os.environ.items():
        if not k.startswith('BATCHPAR'):
            continue
        key = k.split('BATCHPAR_')[1]
        if key in ['model', 's3_path_out', 'outinfo']:
            continue
        kwargs[key] = v
    return kwargs


def run():
    # Load and shape the data
    s3_path_in = os.environ['BATCHPAR_s3_path_in']
    s3 = boto3.resource('s3')
    s3_obj_in = s3.Object(*parse_s3_path(s3_path_in))

    # Prepare the data for processing
    le = LabelEncoder()
    X = json.load(s3_obj_in.get()['Body'])
    y = le.fit_transform([row.pop('sic') for row in X])
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1)

    # 
    Model = retrieve_model()
    model = Model(**kwargs)
    model.fit(X_train, y_train)
    y_pred = model.predict(X)
    y_test_pred = model.predict(X_test)
    f1_score = f1_score(y_test, y_test_pred, average='weighted')

    # Curate the output
    output = {'loss': f1_score,
              'data': {'predicted_labels': y_pred}}

    # Mark the task as done and save the data
    if "BATCHPAR_outinfo" in os.environ:
        s3_path_out = os.environ["BATCHPAR_outinfo"]
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path_out))
        s3_obj.put(Body=json.dumps(output))


if __name__ == "__main__":
    run()
