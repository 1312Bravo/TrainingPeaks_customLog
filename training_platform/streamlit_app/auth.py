import streamlit as st

from config import OWNER_EMAIL


# ----------------------------------------------------------
# Access Mode Helpers
# ----------------------------------------------------------

# Checks whether Streamlit auth has been configured in secrets.
# When it is not configured, the app should still run in public demo mode.
# Returns True when an [auth] section exists.

def is_auth_configured() -> bool:
    try:
        return "auth" in st.secrets
    except Exception:
        return False


# Checks whether the current browser session is logged in through Streamlit auth.
# Streamlit only exposes is_logged_in after auth is configured.
# Returns True only for authenticated sessions.

def is_logged_in() -> bool:
    return bool(getattr(st.user, "is_logged_in", False))


# Checks whether Streamlit auth identifies the owner account.
# Unknown users and non-owner accounts stay in demo mode.
# Returns True only when the logged-in email matches OWNER_EMAIL.

def is_owner() -> bool:
    # Guard against local/no-auth Streamlit runs and non-owner accounts.
    if not is_logged_in():
        return False

    return st.user.get("email") == OWNER_EMAIL


# Renders login/logout controls without blocking demo mode.
# Public users can keep using demo mode, while the owner can log in for full mode.
# Returns nothing; it writes compact auth controls in the top utility row.

def render_auth_controls() -> None:
    if not is_auth_configured():
        st.markdown(
            """
            <div class="tp-access-inline">
                <span class="tp-access-status tp-access-demo">No login | Demo mode</span>
                <span class="tp-access-email">Google login not configured</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    if is_logged_in():
        email = st.user.get("email", "Logged in")
        mode_label = "Owner account | Full mode" if is_owner() else "Guest account | Demo mode"
        mode_class = "tp-access-full" if is_owner() else "tp-access-demo"
        label_column, button_column = st.columns([2.2, 1], gap="small")

        with label_column:
            st.markdown(
                f"""
                <div class="tp-access-inline">
                    <span class="tp-access-status {mode_class}">{mode_label}</span>
                    <span class="tp-access-email">{email}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with button_column:
            st.button("Log out", on_click=st.logout, use_container_width=True)
    else:
        label_column, button_column = st.columns([2.2, 1], gap="small")

        with label_column:
            st.markdown(
                """
                <div class="tp-access-inline">
                    <span class="tp-access-status tp-access-demo">No login | Demo mode</span>
                    <span class="tp-access-email">Owner login unlocks full mode</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with button_column:
            st.button("Log in", on_click=st.login, use_container_width=True)
