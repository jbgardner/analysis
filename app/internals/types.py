from pydantic import BaseModel


class CheckoutSession(BaseModel):
    user_id: str
    plan: str


class RequestOtp(BaseModel):
    phone: str


class VerifyOtp(RequestOtp):
    code: str
