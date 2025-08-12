"""Custom configuration for the Airflow webserver with SSO/AAD."""

from __future__ import annotations

import logging
import os

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from flask_appbuilder.security.manager import AUTH_OAUTH

from airflow.providers.fab.auth_manager.models import Role


SESSION_COOKIE_SECURE = True
PREFERRED_URL_SCHEME = 'https'
BASE_URL = os.environ.get('BASE_URL', 'https://localhost:8080')

# baseado no inssue https://github.com/apache/airflow/issues/49781
from airflow.configuration import conf
from flask import current_app
from werkzeug.middleware.proxy_fix import ProxyFix

if conf.getboolean('fab', 'ENABLE_PROXY_FIX'):
    current_app.wsgi_app = ProxyFix(
        current_app.wsgi_app,
        x_for=conf.getint('fab', 'PROXY_FIX_X_FOR', fallback=1),
        x_proto=conf.getint('fab', 'PROXY_FIX_X_PROTO', fallback=1),
        x_host=conf.getint('fab', 'PROXY_FIX_X_HOST', fallback=1),
        x_port=conf.getint('fab', 'PROXY_FIX_X_PORT', fallback=1),
        x_prefix=conf.getint('fab', 'PROXY_FIX_X_PREFIX', fallback=1),
    )


basedir = os.path.abspath(os.path.dirname(__file__))

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

AUTH_TYPE = AUTH_OAUTH
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLE_ADMIN = 'Admin'
AUTH_ROLES_MAPPING = {
    'Viewer': ['Viewer']
}

# Will allow user self registration
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = 'Viewer'


sso_client_id = os.environ['SSO_CLIENT_ID']
sso_client_secret = os.environ['SSO_CLIENT_SECRET']
sso_tenant_id = os.environ['SSO_TENANT_ID']
aad_url = f'https://login.microsoftonline.com/{sso_tenant_id}/oauth2/v2.0'


OAUTH_PROVIDERS = [
    {
        'name': 'azure',
        'token_key': 'access_token',
        'icon': 'fa-windows',
        'remote_app': {
            'client_id': sso_client_id,
            'client_secret': sso_client_secret,
            'api_base_url': 'https://graph.microsoft.com/v1.0/',
            'access_token_url': f'{aad_url}/token',
            'authorize_url': f'{aad_url}/authorize',
            #'jwks_uri': f'{aad_url}/keys',
            'jwks_uri': f'https://login.microsoftonline.com/{sso_tenant_id}/discovery/v2.0/keys',
            'redirect_uri': f'{BASE_URL}/auth/oauth-authorized/azure',
            'client_kwargs': {
                'scope': 'openid profile email',
                'token_endpoint_auth_method': 'client_secret_post',
            },
        },
    }
]


class CustomSecurityManager(FabAirflowSecurityManagerOverride):
    def get_roles_from_keys(self, role_keys: list[str]) -> set[Role]:
        """
        Construct a list of FAB role objects, from a list of keys.

        NOTE:
        - keys are things like: "LDAP group DNs" or "OAUTH group names"
        - we use AUTH_ROLES_MAPPING to map from keys, to FAB role names

        :param role_keys: the list of FAB role keys
        """
        roles = set()

        for role_key in role_keys:
            fab_role = self.find_role(role_key)

            if fab_role:
                roles.add(fab_role)
                logging.debug('Found role %s', fab_role.name)
            else:
                logging.warning(
                    "Can't find role '%s' specified in AUTH_ROLES_MAPPING: %s",
                    role_key,
                )
        return roles

    def get_oauth_user_info(self, provider, response=None):
        """Extrai as informações do usuário autenticado e cria um usuário automaticamente no Airflow."""
        if provider == 'azure':
            me = self.appbuilder.sm.oauth_remotes[provider].get('me')
            data = me.json()

            me2 = self._decode_and_validate_azure_jwt(response['id_token'])
            roles = me2.get('roles', [])

            email = (
                data.get('mail')
                or data.get('upn')
                or data.get('userPrincipalName')
            )
            username = me2.get('oid', data.get('id', email))
            first_name = data.get('givenName', email.split('@')[0])
            last_name = data.get('surname', '')

            return {
                'username': username,
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'role_keys': roles,
            }


SECURITY_MANAGER_CLASS = CustomSecurityManager
