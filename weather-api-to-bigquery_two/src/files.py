import os
from pyunpack import Archive

def create_folder(out_folder):   
    '''
    create file if not exists  
    arg: 
        1. out_folder= output directory

    return:
            create new directory if not exists
    '''

    if not os.path.exists(out_folder):
        os.mkdir(out_folder)

def extract_file(out_folder,out_folder_as_filename=False):
    '''
    extract compressed file (zip,tar,tar.gz,rar)

    arg:
        1. filename = name of compressed file
        2. url = dawnload url (default none)
        3. dir = file directory
    '''

    archive_types= ['.7z' , '.ace' , '.alz' , '.a' , '.arc' , '.arj' , '.bz2' , '.cab' , '.Z', 
                    '.cpio' , '.deb' , '.dms', '.gz' , '.lrz' ,'.lha', '.lzh' , '.lz' ,'.lzma' ,
                     '.lzo' , '.rpm' , '.rar' , '.rz' , '.tar' , '.xz' , '.zip', '.jar' , '.zoo']
                     
    for flname in os.listdir(out_folder):
        if flname.endswith(*archive_types):
            name= os.path.splitext(os.path.basename(flname))[0]   # its spliting basename from file such('flname','zip')
            if not os.path.isdir(name) :                          # scan the folder named name
                try:
                    file_name=os.path.join(out_folder,flname)      
                    archive=Archive(file_name)

                    if out_folder_as_filename == True:            # extract file into new subfolder
                        new_folder=os.path.join(out_folder,name)
                        os.mkdirs(new_folder)

                        archive.extractall(new_folder)                  
                    
                    else:                                         # if arg is false extract to out_folder
                        archive.extractall(out_folder)
                        print('extraction {} to {} complete'.format(flname,new_folder))               
                
                except BadZipFile:
                    print('Unable to extract this file: {}, check the source it may be corrupt'.format(flname))

                    try: os.remove(file_name)

                    except OSError:

def save_file(data,name_of_file,output_folder):





