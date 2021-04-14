using System.Collections.Generic;
using System.Linq;

namespace EqClient.Services
{
    public class CalculationService
    {
        private Dictionary<char, int> GetVariables(string formula, List<int> values)
        {
            if (string.IsNullOrEmpty(formula)) return new Dictionary<char, int>();

            var operators = new List<char> { '+', '-', '*', '/', '^', '%', '(', ')' };

            var result = new Dictionary<char, int>();

            int valIndex = 0;

            foreach (var ch in formula.Where(ch => !operators.Contains(ch)))
            {
                result.TryAdd(ch, values[valIndex]);
                valIndex++;
            }

            return result;
        }

        private int Calculate(string formula, Dictionary<char, int> values)
        {
            var operators = new List<char> { '+', '-', '*', '/', '^', '%', '(', ')' };

            var tempResult = 0;

            var firstVar = 0;
            var secondVar = 0;
            char op = '#';

            foreach (var ch in formula)
            {

                if (!operators.Contains(ch))
                {
                    if (firstVar == 0 && secondVar == 0)
                    {
                        firstVar = values[ch];
                    }
                    else if (firstVar != 0 && secondVar == 0) secondVar = values[ch];
                }
                else
                {
                    op = ch;
                }

                if (firstVar != 0 && secondVar != 0 && !op.Equals('#'))
                {
                    tempResult = Operation(firstVar, secondVar, op);
                    firstVar = tempResult;
                    secondVar = 0;
                    op = '#';
                }
            }

            return tempResult;
        }

        private int Operation(int first, int second, char operation)
        {
            switch (operation)
            {
                case '+':
                    return first + second;
                case '-':
                    return first - second;
                case '*':
                    return first * second;
                case '/':
                    return first / second;
            }

            return -1;
        }
    }
}
