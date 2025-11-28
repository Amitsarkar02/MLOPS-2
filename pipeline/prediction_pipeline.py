from config.paths_config import *
from utils.helpers import *

def hybrid_recommendation(user_id, user_weight=0.5, content_weight=0.5):

    ### 1️⃣ USER RECOMMENDATION (Collaborative Filtering)
    similar_users = find_similar_users(
        user_id, USER_WEIGHTS_PATH, USER2USER_ENCODED, USER2USER_DECODED
    )

    user_pref = get_user_preferences(user_id, RATING_DF, DF)

    # If user has no preferences, return empty
    if user_pref is None or user_pref.empty:
        return []

    user_recommended_animes = get_user_recommendations(
        similar_users, user_pref, DF, SYNOPSIS_DF, RATING_DF
    )

    # FIX 1: If no recommendations → return empty
    if user_recommended_animes is None or user_recommended_animes.empty:
        return []

    # FIX 2: Ensure column exists
    if "anime_name" not in user_recommended_animes.columns:
        return []

    user_recommended_anime_list = user_recommended_animes["anime_name"].tolist()

    ### 2️⃣ CONTENT RECOMMENDATION (Content-Based Filtering)
    content_recommended_animes = []

    for anime in user_recommended_anime_list:
        similar_animes = find_similar_animes(
            anime,
            ANIME_WEIGHTS_PATH,
            ANIME2ANIME_ENCODED,
            ANIME2ANIME_DECODED,
            DF
        )

        if similar_animes is not None and not similar_animes.empty:
            content_recommended_animes.extend(similar_animes["name"].tolist())

    ### 3️⃣ HYBRID SCORING
    combined_scores = {}

    # Weight from collaborative filtering
    for anime in user_recommended_anime_list:
        combined_scores[anime] = combined_scores.get(anime, 0) + user_weight

    # Weight from content-based filtering
    for anime in content_recommended_animes:
        combined_scores[anime] = combined_scores.get(anime, 0) + content_weight

    # Sort based on combined score
    sorted_animes = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)

    # Return top 10 anime names
    return [anime for anime, score in sorted_animes[:10]]

