import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def missing_value_plot(counts,total_length):
    """
    Input: dict
    """
    plt.figure(figsize=(10,8))
    plt.barh(*zip(*dict(counts).items()),
                alpha=0.7, color="Blue")
    plt.gca().invert_yaxis()
    plt.title('Number of Missing Values')
    plt.ylabel('Column')
    plt.xlabel('Frequency'.format(total_length))
    plt.axvline(x=total_length, linestyle='--', color="grey",alpha=0.6)
    plt.text(total_length-0.5,39, "Max: {}".format(total_length), color="grey",alpha=0.6)
    # plt.xticks(rotation=90)
    plt.grid(alpha=0.1)
    plt.show()


def missing_value_column_count_plot(out_counts):
    plt.figure(figsize=(10,8))
    ax = sns.barplot(["0-10%", "11-20%", "21-30%", "31-40%", "41-50%","51-60%", "61-70%", "71-80%", "81-90%", "91-100%"],
            out_counts.values, alpha=0.9, color="Blue")

    plt.grid(alpha=0.1)
    plt.xticks(fontsize= 'small')
    plt.suptitle("Missing Value Distribution")
    plt.xlabel("% of Missing Values (In Each Column)")
    plt.ylabel("Number of Columns")
    plt.show()

def missing_value_count_pair_plot(pair_dataframe):
    pair_dataframe=pair_dataframe.where(np.triu(np.ones(pair_dataframe.shape)).astype(np.bool)).T
    plt.figure(figsize=(15,8))
    sns.heatmap(pair_dataframe)
    plt.xticks(rotation=295, ha = 'left')
    plt.show()
