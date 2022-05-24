#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
int main(int argc, char *argv[]){

        int fd, ret;
        while(1)
        {
                fd=open("/var/lib/www/html/mydir", O_DIRECTORY | O_RDONLY);
                if(fd < -1)
                {
                perror("creat()");

                }
                fsync(fd);
                close(fd);
        }
        return 9;

}
