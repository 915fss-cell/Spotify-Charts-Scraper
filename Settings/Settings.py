from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SpotifySettings(BaseSettings):
    client_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("SPOTIFY_CLIENT_ID", "CLIENT_ID", "client_id"),
    )
    client_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices("SPOTIFY_CLIENT_SECRET", "CLIENT_SECRET", "client_secret"),
    )
    redirect_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("SPOTIFY_REDIRECT_URI", "REDIRECT_URL", "redirect_url"),
    )
    redirect_port: int | None = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
