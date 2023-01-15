import os

import tekore as tk
from os import environ as env
import httpx
import pickle


def fracture(lst: list, size: int | None = 100, with_idx=False) -> list | tuple[list, int]:
    for i in range(0, len(lst), size):
        if with_idx:
            yield lst[i:i + size], i
        else:
            yield lst[i:i + size]


def lp(name, default=None):
    try:
        with open(f'./pickles/{name}.pickle', mode='rb') as file:
            return pickle.load(file)
    except FileNotFoundError:
        return default


def sp(data, name):
    try:
        with open(f'./pickles/{name}.pickle', mode='wb') as file:
            pickle.dump(data, file)
    except FileNotFoundError:
        os.mkdir('./pickles')
        with open(f'./pickles/{name}.pickle', mode='wb') as file:
            pickle.dump(data, file)


class SuperCred:
    def __init__(self):
        self.client_id = env.get('SPOTIFY_ID')
        self.secret = env.get('SPOTIFY_SECRET')
        self.uri = env.get('SPOTIFY_URI')

    @property
    def cred(self):
        return tk.RefreshingCredentials(self.client_id, self.secret, self.uri, sender=tk.RetryingSender(2))


sc = SuperCred()


class Host:
    def __init__(self):
        self._refresh_token = env.get('SPOTIFY_HOST_TOKEN')
        self._token = None
        self._sp = tk.Spotify(self.token, max_limits_on=True, sender=tk.RetryingSender(retries=2))
        self.user = self.sp.current_user()

    @property
    def token(self):
        if not self._token or self._token.is_expiring:
            self._token = sc.cred.refresh_user_token(self._refresh_token)
        return self._token

    @property
    def sp(self):
        if self._token.is_expiring:
            self._sp = tk.Spotify(self.token, max_limits_on=True, sender=tk.RetryingSender(retries=2))
        return self._sp

    @property
    def asp(self):
        # FIXME: This feels wrong
        trans = httpx.AsyncHTTPTransport(retries=3)
        client = httpx.AsyncClient(timeout=20, transport=trans)
        sender = tk.RetryingSender(retries=3, sender=tk.AsyncSender(client=client))
        return tk.Spotify(self.token, sender=sender, max_limits_on=True)
