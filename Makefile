all:
	g++ -O2 main.cpp file_sorter.cpp -lpthread -o file_sort

debug:
	g++ -g -O2 main.cpp file_sorter.cpp -lpthread -o file_sort
