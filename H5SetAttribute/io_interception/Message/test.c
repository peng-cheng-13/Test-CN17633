#include "stdio.h"
#include "stdlib.h"
#include "atomicops.h"
#include "readerwriterqueue.h"

int main() {
  moodycamel::ReaderWriterQueue<int> q(100);
  q.enqueue(17);
  bool succeeded = q.try_enqueue(18);
  if (succeeded)
    printf("Enqueue succeeded\n");

  int number;
  succeeded = q.try_dequeue(number);
  if (succeeded && number == 17)
   printf("Dequeue succeeded, number is %d\n", number);

  int* front = q.peek();
  if (*front == 18)
    printf("Front dequeue succeeded, number is %d\n", *front);

  return 0;
}
