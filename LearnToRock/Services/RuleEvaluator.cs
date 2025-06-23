public class RuleEvaluator
{
    public string EvaluateLoanEligibility(Customer customer)
    {
        return (customer.Age, customer.Income) switch
        {
            (var age, var income) when age < 18 => "Not eligible: Age must be 18 or older.",
            (var age, var income) when income < 30000 => "Not eligible: Income must be at least $30,000.",
            (var age, var income) when customer.IsMember && customer.CreditScore >= 700 => "Eligible for loan with member benefits.",
            (var age, var income) when !customer.IsMember && customer.CreditScore >= 700 => "Eligible for loan with standard terms.",
            _ => "Not eligible: Check other criteria."
        };
    }

    public string EvaluateDiscount(Customer customer)
    {
        return (customer.Age, customer.Income, customer.IsMember) switch
        {
            (var age, var income, true) when age < 25 => "Eligible for 20% discount for young members.",
            (var age, var income, true) when income > 50000 => "Eligible for 15% discount for high-income members.",
            (var age, var income, false) when age >= 25 && income < 50000 => "Eligible for 10% discount for non-members.",
            _ => "No discount available."
        };
    }
    public string EvaluateCreditScore(Customer customer)
    {
        return customer.CreditScore switch
        {
            < 600 => "Poor credit score: Consider improving your credit history.",
            >= 600 and < 700 => "Fair credit score: You may qualify for some loans.",
            >= 700 and < 800 => "Good credit score: You are likely to get favorable loan terms.",
            _ => "Excellent credit score: You are eligible for the best loan offers."
        };
    }
    public string EvaluateRisk(Customer customer)
    {
        return (customer.Age, customer.Income, customer.CreditScore) switch
        {
            (var age, var income, var creditScore) when age < 18 => "High risk: Age below 18.",
            (var age, var income, var creditScore) when income < 30000 => "Medium risk: Low income.",
            (var age, var income, var creditScore) when creditScore < 600 => "High risk: Poor credit score.",
            (var age, var income, var creditScore) when creditScore >= 600 && creditScore < 700 => "Medium risk: Fair credit score.",
            _ => "Low risk: Good financial profile."
        };
    }
}