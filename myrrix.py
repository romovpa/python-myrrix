import requests
import werkzeug.datastructures


class MyrrixClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def _make_request(self, method, path, params=None, data=None, lines_of_float=False):
        methods = {
            'GET': requests.get,
            'POST': requests.post,
            'DELETE': requests.delete,
        }
        assert method in methods
        method_func = methods[method]

        url = 'http://%s:%d/%s' % (self.host, self.port, path)
        headers={'Accept': 'application/json'}

        resp = method_func(url, params=params, headers=headers)
        if not resp.ok:
            return None
        if lines_of_float:
            return map(float, resp.content.strip().split('\n'))
        return resp.json()

    def add_preference(self, user_id, item_id, strength=None):
        """
        Adds to a user-item association. (The "Set" terminology is retained for consistency with Apache Mahout, but,
        this adds to rather than sets the association.) This is called in response to some action that indicates the
        user has a stronger association to an item, like a click or purchase. It is intended to be called many times for
        a user and item, as more actions are observed that associate the two.

        Value is not a rating, but a strength indicator. It may be negative. Its magnitude should correspond to the
        degree to which an observed event suggests an association between a user and item. A value twice as big should
        correspond to an event that suggests twice as strong an association.

        For example, a click on a video might result in a call with value 1.0. Watching half of the video might result
        in another call adding value 3.0. Finishing the video, another 3.0. Liking or sharing the video, an additional
        10.0. Clicking away from a video within 10 seconds might result in a -3.0.
        """
        if strength is not None:
            strength = str(strength)
        self._make_request('POST', 'pref/%d/%d' % (user_id, item_id), data=strength)

    def remove_preference(self, user_id, item_id):
        """
        This removes the item from the user's set of known items, making it eligible for recommendation again. If the
        user has no more items, this method will remove the user too, such that new calls to recommend for example will
        fail with a 404 Not Found error. It does not record any change in strength of association, meaning, the removal
        does not affect the user's perceived tastes and preferences in the model. Set / Add can do that.
        """
        self._make_request('DELETE', 'pref/%d/%d' % (user_id, item_id))

    def set_user_tag(self, user_id, tag, strength):
        """
        Adds to a user-tag association, where a "tag" can be any string, and represents a concept like a label or
        category. For example many users can be tagged "female" with this API. It operates in every respect like
        Set / Add Preference, including the strength value, except that the target "tag" is not returned in results.
        """
        if strength is not None:
            strength = str(strength)
        self._make_request('POST', 'tag/user/%d/%s' % (user_id, tag), data=strength)

    def set_item_tag(self, item_id, tag, strength):
        """
        Entirely analogous to Set User Tag, except tags items instead of users.
        """
        if strength is not None:
            strength = str(strength)
        self._make_request('POST', 'tag/item/%d/%s' % (item_id, tag), data=strength)

    def ingest(self, preferences):
        """
        Supports bulk-loading new preferences.

        New preferences as a list of the tuples (userID,itemID[,value]). Value is optional and defaults to 1.0.
        """
        data = '\n'.join(','.join(map(str, entry)) for entry in preferences)
        self._make_request('POST', 'ingest', data=data)

    def _recommend_params(self, how_many=None, consider_known_items=None, rescorer_params=None):
        params = werkzeug.datastructures.MultiDict({
            'howMany': how_many,
            'considerKnownItems': consider_known_items,
        })
        if rescorer_params is not None:
            for value in rescorer_params:
                params.add('rescorerParams', value)
        return params

    def recommend(self, user_id, **kwargs):
        """
        Calculates the items that should be most highly recommended to a user. The result is a list of items, ordered by
        a quality score. The value of the quality score is opaque; larger means a better recommendation.

        Arguments

        howMany: Maximum number of recommendations to return. Optional. Defaults to 10.
        considerKnownItems: Whether to consider user's known items as candidates for recommendation. Optional.
                            Defaults to false.
        rescorerParams: Optional parameters to the rescorer. May be repeated.
        """
        result = self._make_request('GET', 'recommend/%d' % user_id, self._recommend_params(**kwargs))
        return map(tuple, result)

    def recommend_to_many(self, user_ids, **kwargs):
        """
        Same as Recommend, but computes recommendations for a group of users, instead of one. Each user is given equal
        weight.
        """
        params = self._recommend_params(**kwargs)
        result = self._make_request('GET', 'recommendToMany/' + '/'.join(map(str, user_ids)), params)
        return map(tuple, result)

    def recommend_to_anonymous(self, preferences, **kwargs):
        """
        This method is a convenience method for recommending to an "anonymous" user that is not already known to the
        system. Instead, the user's associated items are sent with the request and recommendations computed strictly
        from this information.

        Preferences as a list containing itemID or tuple (itemID, strength).
        """
        params = self._recommend_params(**kwargs)
        preferences_string = '/'.join('='.join(map(str, preference)) for preference in preferences)
        result = self._make_request('GET', 'recommendToAnonymous/' + preferences_string, params)
        return map(tuple, result)

    def most_similar_items(self, item_ids, **kwargs):
        """
        Computes the items most similar to an item or group of items. The result is a list of items, ordered by
        similarity. The similarity value is opaque; larger means more similar.
        """
        if not isinstance(item_ids, list):
            item_ids = [item_ids]
        params = self._recommend_params(**kwargs)
        return self._make_request('GET', 'similarity/' + '/'.join(map(str, item_ids)), params)

    def similarity_to_item(self, to_item_id, item_ids):
        """
        Computes the similarity to an item of a given set of other items. The result is a list of similarity values in
        the same order. The similarity value is opaque; larger means more similar.
        """
        if not isinstance(item_ids, list):
            item_ids = [item_ids]
        return self._make_request('GET', 'similarityToItem/%d/' % to_item_id + '/'.join(map(str, item_ids)))

    def estimate(self, user_id, item_ids):
        """
        Estimate the strength of the preference, or association, between a user and an item. The strength value that is
        returned is opaque; higher is stronger. The value may be compared to values returned from the Recommend
        endpoint. One, or more, estimates may be computed at once from this API method.
        """
        if not isinstance(item_ids, list):
            item_ids = [item_ids]
        return self._make_request('GET', 'estimate/%d/' % user_id + '/'.join(map(str, item_ids)), lines_of_float=True)

    def estimate_for_anonymous(self, to_item_id, preferences):
        """
        Estimate the strength of the preference, or association, between an anonymous user and an item. Just as
        Recommend To Anonymous is to Recommend, this counterpart to Estimate accepts not only an item, but a series of
        items (with optional strength values) that an anonymous user has interacted with.
        """
        preferences_string = '/'.join('='.join(map(str, preference)) for preference in preferences)
        return self._make_request('GET', 'estimateForAnonymous/%d/' % to_item_id + preferences_string)

    def because(self, user_id, item_id, how_many=None):
        """
        Attempts to explain why a certain item was recommended to a user. The response contains items that the user is
        associated to which most contributed to the recommendation. They are ordered from most explanatory to least and
        are accompanied by a strength value. The value is opaque.

        Arguments

        howMany: Maximum number of items to return. Optional. Defaults to 10.
        """
        params = {'howMany': how_many}
        result = self._make_request('GET', 'because/%d/%d' % (user_id, item_id), params)
        return map(tuple, result)

    def most_popular_items(self, **kwargs):
        """
        Computes the items most popular overall (interacted with by the most users). The result is a list of items,
        ordered by similarity. The value is currently a count.

        Arguments

        howMany: Maximum number of similar items to return. Optional. Defaults to 10.
        rescorerParams: Optional parameters to the rescorer. May be repeated.
        """
        result = self._make_request('GET', 'mostPopularItems', self._recommend_params(**kwargs))
        return map(tuple, result)

    def refresh(self):
        """
        Requests that the recommender rebuild its internal state and models. It may have no effect; it is only a
        suggestion. The model rebuild proceeds asynchronously and takes effect at some later time.
        """
        self._make_request('POST', 'refresh')

    def is_ready(self):
        """
        Tells whether the Serving Layer is ready to answer requests -- has loaded or computed a model.
        """
        url = 'http://%s:%d/ready' % (self.host, self.port)
        resp = requests.head(url)
        return resp.ok

    def get_all_user_ids(self):
        """
        Retrieves the IDs of all users in the model.
        """
        return self._make_request('GET', 'user/allIDs')


    def get_all_item_ids(self):
        """
        Retrieves the IDs of all items in the model.
        """
        return self._make_request('GET', 'item/allIDs')

