import csv
import math


def load_trusted_handles(csv_path="assets/TrustedSources.csv"):
    trusted = {}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trusted[row["handle"]] = float(row["score"])
    return trusted


TRUSTED_HANDLES = load_trusted_handles()


def compute_quality(node):
    handle = getattr(node, "author_handle")

    if handle in TRUSTED_HANDLES:
        return round(TRUSTED_HANDLES[handle], 3)
    elif getattr(node, "is_trusted_source"):
        return 0.70
    elif getattr(node, "is_domained"):
        return 0.55
    else:
        return 0.20


def compute_virality(node, author_followers=0):
    engagement = 0.7*getattr(node, "likes", 0) + 2 * \
        getattr(node, "reposts", 0) + getattr(node, "replies", 0)

    engagement_score = math.log1p(engagement) / math.log1p(10000)
    follower_score = math.log1p(author_followers) / math.log1p(1_000_000)

    virality = 0.7 * engagement_score + 0.3 * follower_score
    return round(min(virality, 1.0), 3)


def evaluate_node(node, author_followers=0):
    return (compute_quality(node),
            compute_virality(node, author_followers))
