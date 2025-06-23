namespace BankingSimulator
{
    public class BankAccount {
        public int Id { get; private set; }
        private object _lock = new object();
        private decimal _balance;

        public BankAccount(int id, decimal initialBalance = 0){
            Id = id;
            _balance = initialBalance;
        }

        public void Deposit(decimal amount){
            if(amount <= 0) throw new ArgumentException("Deposit amount must be positive.");
            lock(_lock){
                _balance += amount;
                Console.WriteLine($"Deposited {amount:C} to account {Id}. New balance: {_balance:C}");
            }
        }

        public void Withdraw(decimal amount){
            if(amount <= 0) throw new ArgumentException("Withdrawal amount must be positive.");
            lock(_lock){
                if(amount > _balance) throw new InvalidOperationException("Insufficient funds.");
                _balance -= amount;
                Console.WriteLine($"Withdrew {amount:C} from account {Id}. New balance: {_balance:C}");
            }
        }
        public decimal GetBalance(){
            lock(_lock){
                return _balance;
            }
        }
    }
}