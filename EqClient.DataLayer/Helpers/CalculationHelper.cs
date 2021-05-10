using System.Collections.Generic;
using System.Linq;

namespace EqClient.DataLayer.Helpers
{
    public static class CalculationHelper
    {
        private static Dictionary<char, int> GetVariables(string formula, List<int> values)
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

        public static int Calculate(string formula, List<int> rawValues)//Dictionary<char, int> values)
        {
            var values = GetVariables(formula, rawValues);

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

        private static int Operation(int first, int second, char operation)
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
