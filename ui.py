from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def admin_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/sources"), KeyboardButton(text="/add_source")],
            [KeyboardButton(text="/remove_source"), KeyboardButton(text="/set_targets")],
            [KeyboardButton(text="/set_prompt"), KeyboardButton(text="/clear_prompt")],
            [KeyboardButton(text="/set_image_style"), KeyboardButton(text="/clear_image_style")],
            [KeyboardButton(text="/set_image_styles"), KeyboardButton(text="/add_image_style")],
            [KeyboardButton(text="/source_info"), KeyboardButton(text="/backfill")],
            [KeyboardButton(text="/set_image_search")],
        ],
        resize_keyboard=True,
    )
