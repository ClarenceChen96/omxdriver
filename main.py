import tkinter
import os
import subprocess
from omxplayer import OmxDriver
import time
import logging
from playlist import PlayList
from player import Player
import threading
import logging
import yaml
from error_manager import Error_Handler

def read_yaml(filename):
    '''
    loads a yml file for its contained parameters
    :param fn: file name to read
    :return: loaded result as a dictionary
    '''
    with open(filename) as f:
        result = yaml.load(f, Loader=yaml.SafeLoader)
    return result


def callback(root):
    root.quit()

def exit(event = None):
    root.destroy()
    exit(1)
    
def main(args):
    # handler = Error_Handler()
    video_dir = args['video_dir']
    from player import Player
    while True:
        player = Player(video_dir)
        player.initial_load()

if __name__ == '__main__':
    '''
    configuring log module
    '''
    logging.basicConfig(filename='app.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(thread)d - %(levelname)s - %(message)s', level=logging.DEBUG)
    logger = logging.getLogger('logger')

    # this is a experiment for tkinter and canvas

    root = tkinter.Tk()
    screen_width = root.winfo_screenmmwidth()
    screen_height = root.winfo_screenheight()
    print('screen size is ' + str(screen_height) + ' by ' + str(screen_width))
    dict = read_yaml('config.yml')
    num_screens = dict['screen_config']['num_screens']
    if num_screens != len(dict['screen_config']) - 1:
        '''this is a fatal error, report'''
        logger.error('number of screens does not match given number of parameters')
    root.config(bg='black')
    root.attributes('-fullscreen', True)
    root.protocol('WM_DELETE_WINDOW', callback)
    button_canvas = tkinter.Canvas(root, background='black', height=20, width=20)
    button_canvas.place(x=1900, y=1060)
    button = tkinter.Button(button_canvas, text='exit', command=exit)
    button.pack()
    root.bind("<Return>", exit)

    args = {}
    args['video_dir'] = 'video/'
    root.after(1, main(args))
    root.mainloop()
    root = tkinter.Tk()
    root.attributes('-fullscreen', True)
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    #root.geometry('%dx%d%+d%+d' % (screen_width, screen_height, 0, 0))
    canvas = tkinter.Canvas(root, background = 'black', height = screen_height, width = screen_width)
    canvas.place(x = -1, y = -1)
    tkinter.mainloop()
