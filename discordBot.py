import os, sys, time, random, discord, requests, asyncio, logging
from pyjson5 import load as json_load
from combiner import combiner
from editor.download import download
from collections import namedtuple, defaultdict
from func_helper import *
from threading import Thread
from functools import reduce
from operator import add
from editor import editor
from math import ceil

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_handler = logging.StreamHandler(sys.stdout)
logger_handler.setLevel(logging.DEBUG)
logger.addHandler(logger_handler)
info = lambda *args: logger.info(' | '.join(map(str, args)))

config = json_load(open("config.json", 'r'))

message_search_count = config["message_search_count"]
command_chain_limit = config["command_chain_limit"]
working_directory = os.path.realpath(config["working_directory"])
response_messages = config["response_messages"]
max_concat_count = config["max_concat_count"]
discord_tagline = config["discord_tagline"]
discord_token = config["discord_token"]
meta_prefixes = config["meta_prefixes"]
cookie_file = config["cookie_file"] if "cookie_file" in config else None
valid_video_extensions = ["mp4", "webm", "avi", "mkv", "mov"]
valid_image_extensions = ["png", "gif", "jpg", "jpeg"]
valid_extensions = valid_video_extensions + valid_image_extensions
qued_msg = namedtuple("qued_msg", "context message filepath filename reply edit", defaults=6 * [None])
result = namedtuple("result", "success filename message", defaults=3 * [None])

async_runner = Async_handler()
taskList, messageQue = [], []

intents = discord.Intents.default()
intents.messages = True
intents.members = True
discord_status = discord.Game(name=discord_tagline)
bot = discord.AutoShardedClient(status=discord_status, intents=intents)


class target_group:
    def __init__(self, attachments, reply, channel):
        self.attachments = attachments
        self.channel = channel

    def compile(self):
        k = [];
        [k.append(i) for i in (self.attachments + self.channel) if i not in k]
        return k


def human_size(size,
               units=None):  # https://stackoverflow.com/a/43750422/14501641
    if units is None:
        units = [' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    return str(size) + units[0] if size < 1024 else human_size(size >> 10, units[1:])


def generate_uuid_from_msg(msg_id):
    return f"{msg_id}_{(time.time_ns() // 100) % 1000000}"


def generate_uuid_folder_from_msg(msg_id):
    return f"{working_directory}/{generate_uuid_from_msg(msg_id)}"


def clean_message(msg):
    return msg.replace(chr(8206), '').replace('@', '@' + chr(8206))


async def processQue():
    while True:
        if not len(messageQue):
            await asyncio.sleep(1)
            continue

        res = messageQue.pop(0)

        action = res.context.reply if res.reply else (res.context.edit if res.edit else res.context.channel.send)
        if res.filename:
            if (filesize := os.path.getsize(res.filename)) >= 8 * 1024 ** 2:
                await action(
                    f"Sorry, but the resulting file ({human_size(filesize)}) is over Discord's 8MB upload limit.")
            else:
                with open(res.filename, 'rb') as f:
                    args = [res.message] if res.message else []
                    file_kwargs = {"filename": res.filename} if res.filename else {}
                    if res.message and res.context.content.startswith('!') and (
                            action == res.context.reply and res.context.author.id == bot.user.id):
                        await res.context.delete()
                        await asyncio.sleep(1)
                        await res.context.channel.send(*args, file=discord.File(f, **file_kwargs))
                    else:
                        await action(*args, file=discord.File(f, **file_kwargs))
        else:
            await action(res.message)


async def get_targets(msg, attachments=True, reply=True, channel=True, message_search_count=8, stop_on_first=True):
    msg_attachments, msg_reply, msg_channel = [], [], []

    async def do_setters():
        nonlocal msg_attachments, msg_reply, msg_channel
        if attachments: msg_attachments = msg.attachments
        if stop_on_first and msg_attachments: return

        if reply and msg.reference: msg_reply = (await msg.channel.fetch_message(msg.reference.message_id)).attachments
        if stop_on_first and msg_reply: return

        if channel and message_search_count > 0: msg_channel = reduce(add,
                                                                      [i.attachments async for i in msg.channel.history(
                                                                          limit=message_search_count)])

    await do_setters()

    return target_group(msg_attachments, msg_reply, msg_channel)


def download_discord_attachment(target, filename, keep_ext=False):
    if keep_ext:
        filename = f"{filename}.{os.path.splitext(target.filename)[1][1:]}"
    with open(filename, 'wb') as f:
        f.write(requests.get(target.url).content)
    return filename


def download_discord_attachments(targets, folder):
    if not os.path.isdir(folder):
        os.makedirs(folder)
    return [download_discord_attachment(t, folder + '/' + generate_uuid_from_msg(t.id), keep_ext=True) for t in targets]


async def prepare_VideoEdit(msg):
    targets = (await get_targets(msg, message_search_count=message_search_count)).compile()
    if not targets:
        await msg.channel.send("Unable to find a message to edit, maybe upload a video and try again?")
        return

    file_ext = os.path.splitext(targets[0].filename)[1][1:]
    if file_ext not in valid_extensions:
        await msg.channel.send(f"File type not valid, valid file types are: `{'`, `'.join(valid_extensions)}`")
        return

    return targets[0], f"{generate_uuid_folder_from_msg(msg.id)}.{file_ext}"


async def prepare_concat(msg, args):
    concat_count, *name_spec = params if len(params := args.split()) else '2'
    try:
        concat_count = min(max_concat_count,
                           max(2, (int(concat_count) if len(concat_count.strip()) else len(msg.attachments))))
    except Exception as err:
        await msg.reply(f'No video amount given, interpreting "{concat_count}" as specifier...')
        name_spec.insert(0, concat_count)
        concat_count = min(max_concat_count, max(2, len(name_spec)))

    targets_unsorted = list(filter(
        lambda t: os.path.splitext(t.filename)[1][1:] in valid_video_extensions,
        (await get_targets(msg, message_search_count=message_search_count, stop_on_first=False)).compile()
    ))[:concat_count]

    if len(targets_unsorted) < 2:
        await msg.reply("Unable to find enough videos to combine.")
        return

    targets = []
    for s in map(lambda c: c.strip().lower(), name_spec):
        i = 0
        while i < len(targets_unsorted):
            if targets_unsorted[i].filename.lower().startswith(s):
                targets.append(targets_unsorted.pop(i))
            i += 1
    targets += targets_unsorted

    return targets


def process_result_post(msg, res, filename="video.mp4", prefix=None, random_message=True):
    if res.success:
        text = random.choice(response_messages) if random_message else res.message
        content = f"{prefix.strip()} ║ {text.strip()}" if prefix else text.strip()
        messageQue.append(qued_msg(context=msg, filepath=res.filename, filename=filename, message=content, reply=True))
    else:
        messageQue.append(qued_msg(context=msg, message=res.message, reply=True))


async def parse_command(message):
    if message.author.id == bot.user.id and not '║' in message.content:
        return
    msg = message.content.split('║', 1)[0]
    if len(msg) == 0: return

    is_reply_to_bot = message.reference and (
        await message.channel.fetch_message(message.reference.message_id)).author.id == bot.user.id
    if message.author.id != bot.user.id and not is_reply_to_bot and msg.split('>>')[0].removeprefix('!').strip() == "":
        return

    command, *remainder = msg.split(">>")
    spl = command.strip().split(' ', 1)
    cmd = spl[0].strip().lower()
    args = spl[1].strip() if len(spl) > 1 else ""

    final_command_name = None
    if cmd in ["concat", "combine"]:
        final_command_name = "concat"
    elif cmd in ["download", "downloader"]:
        final_command_name = "download"
    elif cmd == "help":
        final_command_name = "help"
    elif cmd == "hat":
        final_command_name = "hat"
    elif ev1 := (cmd in ["destroy:", ""]):
        final_command_name = "destroy:"
        if not ev1 or cmd == "":
            args = f"{cmd} {args}"

    if not final_command_name:
        return

    match final_command_name:
        case "help":
            await message.reply(
                "VideoEditBot Command Documentation: https://github.com/thunderzapper/videoEditBot/blob/main/COMMANDS.md")
        case "hat":
            embed = discord.Embed(title='hat', description='hat')
            embed.set_image(
                url="https://cdn.discordapp.com/attachments/748021401016860682/920801735147139142"
                    "/5298188282_1639606638167.png")
            await message.reply("Hat", embed=embed)
        case "concat":
            Task(
                Action(prepare_concat, message, args,
                       name="Concat Command Prep",
                       check=lambda x: x
                       ),
                Action(download_discord_attachments, swap_arg("result"), generate_uuid_folder_from_msg(message.id),
                       name="Download videos to Concat",
                       check=lambda x: x
                       ),
                Action(combiner, swap_arg("result"),
                       (concat_filename := f"{generate_uuid_folder_from_msg(message.id)}.mp4"),
                       SILENCE="./editor/SILENCE.mp3",
                       print_info=False,
                       name="Concat Videos",
                       fail_action=Action(
                           lambda n, e: messageQue.append(
                               qued_msg(
                                   context=message,
                                   message="Sorry, something went wrong during concatenation.",
                                   reply=True
                               )
                           )
                       )
                       ),
                Action(process_result_post, message, result(True, concat_filename, ""), concat_filename, remainder,
                       name="Post Concat"
                       ),
                async_handler=async_runner
            ).run_threaded()
        case "download":
            Task(
                Action(download, download_filename := f"{generate_uuid_folder_from_msg(message.id)}.mp4", args,
                       name="yt-dlp download"),
                Action(process_result_post, message, swap_arg("result"), download_filename, remainder,
                       name="Post Download"
                       ),
            ).run_threaded()
        case "destroy:":
            Task(
                Action(prepare_VideoEdit, message,
                       name="VEB Command Prep",
                       check=lambda x: x,
                       parse=lambda x: {
                           "target": x[0],
                           "filename": x[1]
                       },
                       skip_task_fail_handler=True
                       ),
                Action(download_discord_attachment, swap_arg("target"), swap_arg("filename"),
                       name="VEB Download Target"
                       ),
                Action(editor, swap_arg("filename"), args, workingDir=working_directory, keepExtraFiles=True,
                       name="VEB",
                       parse=lambda x: {
                           "result": x,
                           "filename": x.filename
                       }
                       ),
                Action(process_result_post, message, swap_arg("result"), swap_arg("filename"), remainder,
                       name="VEB Post"
                       ),
                async_handler=async_runner,
                persist_result_values=True
            ).run_threaded()


botReady = False


@bot.event
async def on_ready():
    global botReady, meta_prefixes
    if botReady: pass
    if not os.path.isdir(working_directory):
        os.makedirs(working_directory)

    await bot.change_presence(activity=discord_status)
    asyncio.create_task(processQue())
    asyncio.create_task(async_runner.looper())

    botReady = True
    info("Bot ready!")


@bot.event
async def on_message(msg):
    await parse_command(msg)


bot.run(discord_token)
