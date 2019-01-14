def get_distribution_probabilities(distribution):

    """Function to retrieve the topic distribution probabilities

    Args:
        distribution (list): List of tuples. Each tuple in form of (topic_number, probability)

    Return:
        topic_probability_distribution (list): List of the probability distribution list (in correct for for LDA model inference).

    """

    topic_distribution = []

    for ls in distribution[0]:
        topic_distribution.append(ls[1])

    return [topic_distribution]
