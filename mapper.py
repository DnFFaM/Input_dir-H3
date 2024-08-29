import sys

def map_line(line):
    words = line.split()
    for word in words:
        cleaned_word = word.strip(', . ; : ? ! () [] {}').lower()
        if cleaned_word:
            print(f"{cleaned_word}\t1")

def main():
    for line in sys.stdin:
        map_line(line)

if __name__ == "__main__":
    main()
