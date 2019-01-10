def get_distribution_probabilities(distribution):

    topic_distribution = []

    for ls in distribution[0]:
        topic_distribution.append(ls[1])

    return [topic_distribution]
