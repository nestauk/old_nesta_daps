# cb-health-org-prediction
Predict CB organisations working in health.

### In summary:
* `data.py` synthesises the training set.
* `model.py` trains a Random Forest using grid search.
* `predict.py` takes a **pickled list of the category_list** column from Crunchbase (organisations.csv), vectorises it and produces an array of boolean values (1=health, 0=non-health).

### Instructions
To use the pre-trained model, install the requirements and then use:
```python predict.py PATH_TO_PICKLED_LIST_OF_category_list_column```
