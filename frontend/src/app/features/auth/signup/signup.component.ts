import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, RouterModule } from '@angular/router';
import { AuthService } from '../../../services/auth.service';

@Component({
    selector: 'app-signup',
    standalone: true,
    imports: [CommonModule, FormsModule, RouterModule],
    templateUrl: './signup.component.html',
    styleUrls: ['../login/login.component.css'] // Reuse login styles
})
export class SignupComponent {
    email = '';
    password = '';
    confirmPassword = '';
    errorMessage = '';
    isLoading = false;

    constructor(private authService: AuthService, private router: Router) { }

    onSubmit() {
        if (!this.email || !this.password) return;

        if (this.password !== this.confirmPassword) {
            this.errorMessage = 'Passwords do not match';
            return;
        }

        if (this.password.length < 8) {
            this.errorMessage = 'Password must be at least 8 characters';
            return;
        }

        this.isLoading = true;
        this.errorMessage = '';

        this.authService.signup({ email: this.email, password: this.password }).subscribe({
            next: () => {
                this.isLoading = false;
            },
            error: (err) => {
                console.error('Signup failed', err);
                if (err.status === 409) {
                    this.errorMessage = 'An account with this email already exists.';
                } else {
                    this.errorMessage = err.error?.error || 'Signup failed. Please try again.';
                }
                this.isLoading = false;
            }
        });
    }
}
