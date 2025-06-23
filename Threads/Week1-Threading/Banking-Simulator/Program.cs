
using BankingSimulator;
using System;
namespace BankingSimulator{
public class Program
{
    public static void Main(string[] args)
    {
    // Create a bank account with an initial balance of 1000
    BankAccount account = new BankAccount(1, 1000);

    // Start multiple threads to perform deposits and withdrawals
    Thread depositThread = new Thread(() => {
        for (int i = 0; i < 5; i++)
        {
            account.Deposit(100);
            Thread.Sleep(100); // Simulate some delay
        }
    });

    Thread withdrawThread = new Thread(() => {
        for (int i = 0; i < 5; i++)
        {
            try
            {
                account.Withdraw(50);
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine(ex.Message);
            }
            Thread.Sleep(150); // Simulate some delay
        }
    });

    depositThread.Start();
    withdrawThread.Start();

    depositThread.Join();
    withdrawThread.Join();

    Console.WriteLine($"Final balance: {account.GetBalance():C}");
    }
    }
}