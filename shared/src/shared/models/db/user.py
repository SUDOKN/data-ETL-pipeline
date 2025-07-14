from beanie import Document


class User(Document):
    firstName: str
    lastName: str
    email: str
    companyURL: str
    salt: str
    hashedPassword: str
    resetPwdToken: str | None = None

    class Settings:
        name = "users"
